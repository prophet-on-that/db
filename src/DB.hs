module DB where

import qualified Data.ByteString as B
import qualified StmContainers.Map as Map
import qualified Control.Concurrent.STM.Lock as L
import GHC.Conc (atomically, TVar, newTVar, readTVar, writeTVar, throwSTM, STM)
import System.IO (Handle, openBinaryFile, IOMode(..), hSeek, SeekMode(..), hFlush, hClose)
import Data.Serialize (Serialize (..), decode, encode, runGet, runPut)
import GHC.Generics (Generic)
import Control.Exception (Exception, throw)
import Data.Typeable (Typeable)
import Control.Monad (when, forM_, forM)
import ListT (toReverseList)
import Data.List (sortBy, groupBy)
import Data.Ord (comparing)
import Focus (lookupAndDelete)
import Data.Maybe (catMaybes)
import Page (Page, newPage, pageSize, getPage, putPage)
import Field (FieldSpec)

data DBException
  = PageDecodeError TableId PageId String
  | TableHeaderDecodeError TableId String
  | InvalidPageId TableId PageId
  | TableNotOpen TableId
  | MissingFieldSpec TableId
  | DBInitError FilePath
  deriving (Show, Typeable)

instance Exception DBException

data TableHeader = TableHeader
  { pageCount :: Int
  } deriving (Show, Generic)

instance Serialize TableHeader

-- ^ Size of table header in bytes
tableHeaderSize
  = 8

data TableData = TableData
  { tableHeader :: TableHeader
  , dirtyHeader :: Bool
  , handle :: Handle
  }

data MemPage = MemPage
  { page :: Page
  , isDirty :: Bool
  } deriving (Show)

type TableId = Int

type PageId = Int

type PageMap = Map.Map (TableId, PageId) MemPage

type LockMap = Map.Map TableId L.Lock

type TableMap = Map.Map TableId TableData

type FieldSpecMap = Map.Map TableId FieldSpec

data DB = DB
  { dataDir :: FilePath
  , tableCounter :: TVar Int
  , tableCounterHasChanged :: TVar Bool
  , pageMap :: PageMap
  , lockMap :: LockMap
  , tableMap :: TableMap
  , fieldSpecMap :: FieldSpecMap
  , fieldSpecMapHasChanged :: TVar Bool
  }

defaultDataDir
  = "tmp"

newDB
  :: FilePath -- ^ NOTE: this directory must exist!
  -> IO DB
newDB dataDir
  = atomically $
          DB dataDir
      <$> newTVar 0
      <*> newTVar False
      <*> Map.new
      <*> Map.new
      <*> Map.new
      <*> Map.new
      <*> newTVar False

loadDB :: FilePath -> IO DB
loadDB dataDir = do
  let
    tableCounterFile
      = tableCounterFileName dataDir
    fieldSpecMapFile
      = fieldSpecMapFileName dataDir
  tableCounter <- B.readFile tableCounterFile >>= either (throw $ DBInitError tableCounterFile) return . decode
  fieldSpecList :: [(TableId, FieldSpec)] <- B.readFile fieldSpecMapFile >>= either (throw $ DBInitError fieldSpecMapFile) return . decode
  atomically $ do
    fieldSpecMap <- Map.new
    forM_ fieldSpecList $ \(tableId, fieldSpec) ->
      Map.insert fieldSpec tableId fieldSpecMap
    DB dataDir <$> newTVar tableCounter <*> newTVar False <*> Map.new <*> Map.new <*> Map.new <*> return fieldSpecMap <*> newTVar False

closeDB :: DB -> IO ()
closeDB db@DB {..} = do
  -- TODO: close open connections to the DB (once implemented)
  flushDB db
  -- Close file handles
  handles <- atomically $ do
    locks <- toReverseList . Map.listT $ lockMap
    fmap catMaybes . forM locks $ \(tableId, _) -> do
      Map.delete tableId lockMap
      fmap handle <$> Map.focus lookupAndDelete tableId tableMap
  -- NOTE: we assume there are no open connections so it is safe to
  -- close handles without obtaining locks.
  forM_ handles hClose

getTableFileName :: String -> TableId -> String
getTableFileName dirName tableId
  = dirName ++ "/" ++ show tableId

-- Open a table and load header. Nothing happens if the table is
-- already loaded.
openTable :: DB -> TableId -> IO TableData
openTable DB {..} tableId = do
  lock <- atomically $ do
    existingLock <- Map.lookup tableId lockMap
    case existingLock of
      Nothing -> do
        lock <- L.new
        Map.insert lock tableId lockMap
        return lock
      Just lock' ->
        return lock'
  L.with lock $ do
    -- Check if table has now been loaded
    tableData <- atomically $ Map.lookup tableId tableMap
    case tableData of
      Nothing -> do
        handle <- openBinaryFile (getTableFileName dataDir tableId) ReadWriteMode
        -- TOOD: set buffer mode of handle
        header <- decode <$> B.hGet handle tableHeaderSize
        case header of
          Left err ->
            throw $ TableHeaderDecodeError tableId err
          Right header' -> do
            let
              tableData
                = TableData header' False handle
            atomically $ Map.insert tableData tableId tableMap
            return tableData
      Just tableData' ->
        return tableData'

-- Create a table by opening file handle. Does not write table header
-- to disk.
createTable :: DB -> FieldSpec -> IO (TableId, TableData)
createTable DB {..} fieldSpec = do
  (tableId, lock) <- atomically $ do
    tableId <- readTVar tableCounter
    writeTVar tableCounter $ tableId + 1
    writeTVar tableCounterHasChanged True
    Map.insert fieldSpec tableId fieldSpecMap
    writeTVar fieldSpecMapHasChanged True
    -- Create lock
    lock <- L.new
    Map.insert lock tableId lockMap
    return (tableId, lock)
  L.with lock $ do
    -- NOTE: We do not check for an existing table after acquiring the
    -- lock as the tableId has been incremented atomically.
    handle <- openBinaryFile (getTableFileName dataDir tableId) ReadWriteMode
    -- TOOD: set buffer mode of handle
    let
      tableHeader
        = TableHeader 0
      tableData
        = TableData tableHeader True handle
    atomically $ Map.insert tableData tableId tableMap
    return (tableId, tableData)

-- Alter a page, loading it into memory if not already present. Throws
-- a 'TableNotOpen' exception if the table is not open.
alterPage
  :: DB
  -> TableId
  -> PageId
  -> (MemPage -> MemPage) -- ^ Function to modify page
  -> IO MemPage -- ^ Returns the modified page
alterPage DB {..} tableId pageId alter = do
  pageOrLock <- atomically $ do
    page <- alterPage
    case page of
      Just page' ->
        return $ Left page'
      Nothing -> do
        -- If page not loaded, load from disk. This is
        -- mutually-exclusive per table.
        lock <- Map.lookup tableId lockMap
        maybe (throwSTM $ TableNotOpen tableId) (return . Right) lock
  case pageOrLock of
    Left page' ->
      return page'
    Right lock -> do
      L.with lock $ do
        -- Check to see whether page has now been loaded
        (page, TableData {..}, fieldSpec) <- atomically $ do
          page <- alterPage
          tableData <- Map.lookup tableId tableMap >>= maybe (throwSTM $ TableNotOpen tableId) return
          fieldSpec <- Map.lookup tableId fieldSpecMap >>= maybe (throwSTM $ MissingFieldSpec tableId) return
          return (page, tableData, fieldSpec)
        case page of
          Just page' ->
            return page'
          Nothing -> do
            -- NOTE: this page count check does not guarantee a valid
            -- load, because some pages may exist in the page map but
            -- have not yet been stored to disk.
            when (pageId < 0 || pageCount tableHeader <= pageId) $
              throw $ InvalidPageId tableId pageId
            hSeek handle AbsoluteSeek . toInteger $ tableHeaderSize + pageSize * pageId
            page <- runGet (getPage fieldSpec) <$> B.hGet handle pageSize >>= either (throw . PageDecodeError tableId pageId) return
            -- TODO: evict once page size reaches map limit
            let
              memPage
                = alter $ MemPage page False
            atomically $ Map.insert memPage (tableId, pageId) pageMap
            return memPage
  where
    alterPage :: STM (Maybe MemPage)
    alterPage = do
      page <- Map.lookup (tableId, pageId) pageMap
      case page of
        Just page' -> do
          let
            newPage
              = alter page'
          Map.insert newPage (tableId, pageId) pageMap
          return $ Just newPage
        Nothing ->
          return Nothing

createPage :: DB -> TableId -> STM PageId
createPage DB {..} tableId = do
  tableData@TableData {..} <- Map.lookup tableId tableMap >>= maybe (throwSTM $ TableNotOpen tableId) return
  let
    pageId
      = pageCount tableHeader
    updatedTableData
      = tableData {tableHeader = TableHeader $ pageId + 1}
  Map.insert updatedTableData tableId tableMap
  Map.insert (MemPage newPage True) (tableId, pageId) pageMap
  return pageId

tableCounterFileName
  = (++ "/tableCounter")

fieldSpecMapFileName
  = (++ "/fieldSpecMap")

flushDB :: DB -> IO ()
flushDB DB {..} = do
  (pagesToFlush, tableCounter', fieldSpecMap') <- atomically $ do
    pagesToFlush <- fmap (filter $ isDirty . snd) . toReverseList . Map.listT $ pageMap
    -- Mark all affected pages as written
    forM_ pagesToFlush $ \(tableAndPageId, memPage) ->
      Map.insert (memPage {isDirty = False}) tableAndPageId pageMap

    flushTableCounter <- readTVar tableCounterHasChanged
    tableCounter' <- if flushTableCounter
      then do
        writeTVar tableCounterHasChanged False
        Just <$> readTVar tableCounter
      else
        return Nothing

    flushFieldSpecMap <- readTVar fieldSpecMapHasChanged
    fieldSpecMap' <- if flushFieldSpecMap
      then do
        writeTVar fieldSpecMapHasChanged False
        fmap Just . toReverseList . Map.listT $  fieldSpecMap
      else
        return Nothing

    return (pagesToFlush, tableCounter', fieldSpecMap')

  flushPages pagesToFlush
  -- TODO: the following operations should be locked to not execute
  -- concurrently
  maybe (return ()) flushTableCounter tableCounter'
  maybe (return ()) flushFieldSpecMap fieldSpecMap'
  where
    flushPages pagesToFlush = do
      let
        -- TODO: there must be a better way to write this
        areEqualTables a b
          = (fst . fst) a == (fst . fst) b
        groupedPages
          = groupBy areEqualTables . sortBy (comparing fst) $ pagesToFlush
      forM_ groupedPages $ \pages -> do
        let
          tableId
            = fst . fst . head $ pages
        (lock, TableData {..}, writeTableHeader) <- atomically $ do
          lock <- Map.lookup tableId lockMap >>= maybe (throwSTM $ TableNotOpen tableId) return
          tableData <- Map.lookup tableId tableMap >>= maybe (throwSTM $ TableNotOpen tableId) return
          if dirtyHeader tableData
            then do
              Map.insert (tableData {dirtyHeader = False}) tableId tableMap
              return (lock, tableData, True)
            else
              return (lock, tableData, False)
        -- TODO: unmark pages (and table header if necessary) on exception
        L.with lock $ do
          when writeTableHeader $ do
            hSeek handle AbsoluteSeek 0
            B.hPut handle (encode tableHeader)
          forM_ pages $ \((_, pageId), MemPage {..}) -> do
            hSeek handle AbsoluteSeek . toInteger $ tableHeaderSize + pageSize * pageId
            B.hPut handle . runPut . putPage $ page
          hFlush handle

    flushTableCounter
      = B.writeFile (tableCounterFileName dataDir) . encode

    flushFieldSpecMap
      = B.writeFile (fieldSpecMapFileName dataDir) . encode
