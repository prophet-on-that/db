{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

module DB where

import qualified Data.ByteString as B
import qualified StmContainers.Map as Map
import qualified Control.Concurrent.STM.Lock as L
import GHC.Conc (atomically, TVar, newTVar, readTVar, writeTVar, throwSTM, STM)
import System.IO (Handle, openBinaryFile, IOMode(..), hSeek, SeekMode(..), hFlush, hClose)
import Data.Serialize (Serialize (..), decode, encode, putByteString, Get, Putter)
import GHC.Generics (Generic)
import Control.Exception (Exception, throw)
import Data.Typeable (Typeable)
import Control.Monad (when, forM_, forM)
import ListT (toReverseList)
import Data.List (sortBy, groupBy)
import Data.Ord (comparing)
import Focus (lookupAndDelete)
import Data.Maybe (catMaybes)
import Data.Int (Int32)
import Field (Field, FieldSpec, getField, fieldSpecSize, putField)

data DBException
  = PageDecodeError TableId PageId String
  | TableHeaderDecodeError TableId String
  | InvalidPageId TableId PageId
  | TableNotOpen TableId
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

data PageHeader = PageHeader
  { usedSpace :: Int
  } deriving (Show, Generic)

instance Serialize PageHeader

data Page = Page
  { pageHeader :: PageHeader
  , rows :: [Row]
  } deriving (Show)

-- ^ The size of a table page in bytes, including its header
pageSize :: Int
pageSize
  = 1024

pageHeaderSize
  = 8

pageDataSize :: Int
pageDataSize
  = pageSize - pageHeaderSize

newPage :: Page
newPage
  = Page (PageHeader 0) []

data MemPage a = MemPage
  { page :: a
  , isDirty :: Bool
  } deriving (Show)

type TableId = Int

type PageId = Int

type PageMap = Map.Map (TableId, PageId) (MemPage Page)

type LockMap = Map.Map TableId L.Lock

type TableMap = Map.Map TableId TableData

data DB = DB
  { dataDir :: String
  , tableCounter :: TVar Int
  , pageMap :: PageMap
  , lockMap :: LockMap
  , tableMap :: TableMap
  }

newDB :: IO DB
newDB
  = atomically $
          DB "tmp"              -- NOTE: this directory must already exist!
      <$> newTVar 0 -- TODO: should be read from disk
      <*> Map.new
      <*> Map.new
      <*> Map.new

closeDB :: DB -> IO ()
closeDB db@DB {..} = do
  -- TODO: close open connections to the DB (once implemented)
  flushPages db
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
createTable :: DB -> IO (TableId, TableData)
createTable DB {..} = do
  (tableId, lock) <- atomically $ do
    tableId <- readTVar tableCounter
    writeTVar tableCounter $ tableId + 1
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

-- Load a table page into memory. Throws a 'TableNotOpen' exception if
-- the table is not open.
loadPage :: DB -> TableId -> PageId -> IO Page
loadPage DB {..} tableId pageId = do
  pageOrLock <- atomically $ do
    page <- Map.lookup (tableId, pageId) pageMap
    case page of
      Just page' ->
        return $ Left page'
      Nothing -> do
        -- If page not loaded, load from disk. This is
        -- mutually-exclusive per table.
        lock <- Map.lookup tableId lockMap
        maybe (throwSTM $ TableNotOpen tableId) (return . Right) lock
  case pageOrLock of
    Left MemPage {..} ->
      return page
    Right lock -> do
      L.with lock $ do
        -- Check to see whether page has now been loaded
        (page, TableData {..}) <- atomically $ do
          page <- Map.lookup (tableId, pageId) pageMap
          tableData <- Map.lookup tableId tableMap
          maybe (throwSTM $ TableNotOpen tableId) (return . (page,)) tableData
        case page of
          Just MemPage {..} ->
            return page
          Nothing -> do
            -- NOTE: this page count check does not guarantee a valid
            -- load, because some pages may exist in the page map but
            -- have not yet been stored to disk.
            when (pageCount tableHeader < pageId) $
              throw $ InvalidPageId tableId pageId
            hSeek handle AbsoluteSeek . toInteger $ tableHeaderSize + pageSize * pageId
            -- page <- decode <$> B.hGet handle pageSize >>= either (throw . PageDecodeError tableId pageId) return
            page <- return newPage
            -- TODO: evict once page size reaches map limit
            let
              memPage
                = MemPage page False
            atomically $ Map.insert memPage (tableId, pageId) pageMap
            return page

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

flushPages :: DB -> IO ()
flushPages DB {..} = do
  pagesToFlush <- atomically $ do
    pagesToFlush <- fmap (filter $ isDirty . snd) . toReverseList . Map.listT $ pageMap
    -- Mark all affected pages as written
    forM_ pagesToFlush $ \(tableAndPageId, memPage) ->
      Map.insert (memPage {isDirty = False}) tableAndPageId pageMap
    return pagesToFlush
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
        -- B.hPut handle (encode page)
      hFlush handle

data Row = Row
  { tminCommitted :: Bool
  , tmin :: Int32
  , tmaxCommitted :: Bool
  , tmax :: Int32
  , fields :: [Field]
  } deriving (Show)

getRow :: FieldSpec -> Get Row
getRow fieldSpec
  = Row <$> get
        <*> get
        <*> get
        <*> get
        <*> mapM getField fieldSpec

rowSize :: FieldSpec -> Int
rowSize
  -- TODO: use less space to store row data
  = (10 +) . fieldSpecSize

getRows :: Int -> FieldSpec -> Get [Row]
getRows usedSpace fieldSpec
  = helper 0
  where
    helper countParsed
      | usedSpace == rowSize fieldSpec * countParsed
          = return []
      | usedSpace < rowSize fieldSpec * countParsed
          = fail $  "Mismatch between parsed rows and reported space usage (usedSpace: " ++ show usedSpace ++ ", countParsed: " ++ show countParsed ++ ", rowSize: " ++ show (rowSize fieldSpec) ++ ")"
      | otherwise = do
          row <- getRow fieldSpec
          rows <- helper (countParsed + 1)
          return $ row : rows

getPage :: FieldSpec -> Get Page
getPage fieldSpec = do
  pageHeader@PageHeader {..} <- get
  rows <- getRows usedSpace fieldSpec
  return $ Page pageHeader rows

putPage :: Putter Page
putPage Page {..} = do
  put pageHeader
  mapM_ putRow rows
  putByteString $ B.replicate (pageDataSize - usedSpace pageHeader) 0

putRow :: Putter Row
putRow Row {..} = do
  put tminCommitted
  put tmin
  put tmaxCommitted
  put tmax
  mapM_ putField fields
