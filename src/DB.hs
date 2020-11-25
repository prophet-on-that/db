module DB where

import qualified Data.ByteString as B
import qualified StmContainers.Map as Map
import qualified Control.Concurrent.STM.Lock as L
import GHC.Conc (atomically, TVar, newTVar, readTVar, writeTVar, throwSTM, STM)
import System.IO (Handle, openBinaryFile, IOMode(..), hSeek, SeekMode(..), hFlush, hClose)
import Data.Serialize (Serialize (..), decode, encode, runGet, runPut)
import GHC.Generics (Generic)
import Control.Exception (Exception, throwIO)
import Data.Typeable (Typeable)
import Control.Monad (when, forM_, forM)
import ListT (toReverseList)
import Data.List (sortBy, groupBy)
import Data.Ord (comparing)
import Focus (lookupAndDelete)
import Data.Maybe (catMaybes)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word (Word16, Word32)
import Control.Arrow ((***))
import Page (Page (..), Row (..), pageSize, getPage, putPage, TxId, txIdMin, getRowSize, pageSpace)
import Field (FieldSpec, Field (..), validateFields)

data DBException
  = PageDecodeError TableId PageId String
  | TableHeaderDecodeError TableId String
  | InvalidPageId TableId PageId
  | TableNotOpen TableId
  | MissingFieldSpec TableId
  | DBInitError FilePath
  | InvalidTx TxId
  | InvalidRow FieldSpec [Field]
  deriving (Show, Typeable)

instance Exception DBException

data TableHeader = TableHeader
  { pageCount :: PageId
  } deriving (Show, Generic)

instance Serialize TableHeader

-- ^ Size of table header in bytes
tableHeaderSize :: Word16
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

type TableId = Word16

type PageId = Word32

type PageMap = Map.Map (TableId, PageId) MemPage

type LockMap = Map.Map TableId L.Lock

type TableMap = Map.Map TableId TableData

type FieldSpecMap = Map.Map TableId FieldSpec

data Tx = Tx
  { writtenPages :: Set (TableId, PageId)
  } deriving (Show)

newTx :: Tx
newTx
  = Tx Set.empty

type TxMap = Map.Map TxId Tx

data DB = DB
  { dataDir :: FilePath
  , tableCounter :: TVar TableId
  , tableCounterHasChanged :: TVar Bool
  , pageMap :: PageMap
  , lockMap :: LockMap
  , tableMap :: TableMap
  , fieldSpecMap :: FieldSpecMap
  , fieldSpecMapHasChanged :: TVar Bool
  , txCounter :: TVar TxId
  , txMap :: TxMap
  }

printDB :: DB -> IO ()
printDB DB {..} = do
  (tableCounter', tableCounterHasChanged', tables, fieldSpecs, fieldSpecMapHasChanged, txCounter', txs) <- atomically $ (,,,,,,) <$>
    readTVar tableCounter <*>
    readTVar tableCounterHasChanged <*>
    (toReverseList . Map.listT) tableMap <*>
    (toReverseList . Map.listT) fieldSpecMap <*>
    readTVar fieldSpecMapHasChanged <*>
    readTVar txCounter <*>
    (toReverseList . Map.listT) txMap

  putStrLn $ "Table counter: " ++ show tableCounter' ++ "\tunsaved changes: " ++ show tableCounterHasChanged'

  when (not $ null fieldSpecs) $
    putStrLn "\nTable definitions:"
  forM_ fieldSpecs $ \(tableId, fieldSpec) ->
    putStrLn $ show tableId ++ "\t" ++ show fieldSpec

  when (not $ null tables) $
    putStrLn "\nLoaded tables:"
  forM_ tables $ \(tableId, TableData {..}) ->
    putStrLn $ show tableId ++ "\tpage count: " ++ (show . pageCount) tableHeader ++ "\tdirty header: " ++ show dirtyHeader

  putStrLn $ "\nField spec map unsaved changes: " ++ show fieldSpecMapHasChanged

  putStrLn $ "\nTransaction counter: " ++ show txCounter'

  when (not $ null txs) $
    putStrLn "\nOpen transactions:"
  forM_ txs $ \(tableId, Tx {..}) ->
    putStrLn $ show tableId ++ "\twritten pages: " ++ show writtenPages


defaultDataDir
  = "tmp"

-- TODO: create directory and fail if it already exists (to not
-- overwrite anything)
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
      <*> newTVar txIdMin
      <*> Map.new

loadDB :: FilePath -> IO DB
loadDB dataDir = do
  let
    tableCounterFile
      = tableCounterFileName dataDir
    fieldSpecMapFile
      = fieldSpecMapFileName dataDir
  tableCounter <- B.readFile tableCounterFile >>= either (\_ -> throwIO $ DBInitError tableCounterFile) return . decode
  fieldSpecList :: [(TableId, FieldSpec)] <- B.readFile fieldSpecMapFile >>= either (\_ -> throwIO $ DBInitError fieldSpecMapFile) return . decode
  -- ^ TODO: load tx counter
  atomically $ do
    fieldSpecMap <- Map.new
    forM_ fieldSpecList $ \(tableId, fieldSpec) ->
      Map.insert fieldSpec tableId fieldSpecMap
    DB dataDir <$> newTVar tableCounter <*> newTVar False <*> Map.new <*> Map.new <*> Map.new <*> return fieldSpecMap <*> newTVar False <*> newTVar 0 <*> Map.new

closeDB :: DB -> IO ()
closeDB db@DB {..} = do
  -- TODO: rollback outstanding transactions
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
        header <- fmap decode . B.hGet handle . fromIntegral $ tableHeaderSize
        case header of
          Left err ->
            throwIO $ TableHeaderDecodeError tableId err
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
-- TODO: throw error if field spec size greater than page space
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
  -> (MemPage -> STM (MemPage, a)) -- ^ Function to modify page and return result
  -> IO (MemPage, a) -- ^ Returns result of modify function
alterPage DB {..} tableId pageId modPage = do
  pageOrLock <- atomically $ do
    page <- alterPage'
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
          page <- alterPage'
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
              throwIO $ InvalidPageId tableId pageId
            hSeek handle AbsoluteSeek . toInteger $ tableHeaderSize + pageSize * (fromIntegral pageId)
            page <- runGet (getPage fieldSpec) <$> B.hGet handle (fromIntegral pageSize) >>= either (throwIO . PageDecodeError tableId pageId) return
            -- TODO: evict once page size reaches map limit
            let
              memPage
                = MemPage page False

            atomically $ do
              ret@(newMemPage, _) <- modPage memPage
              Map.insert newMemPage (tableId, pageId) pageMap
              return ret
  where
    alterPage' = do
      memPage <- Map.lookup (tableId, pageId) pageMap
      case memPage of
        Just memPage' -> do
          ret@(newMemPage, _) <- modPage memPage'
          Map.insert newMemPage (tableId, pageId) pageMap
          return $ Just ret
        Nothing ->
          return Nothing

fetchPage
  :: DB
  -> TableId
  -> PageId
  -> IO MemPage
fetchPage db tableId pageId
  = fst <$> alterPage db tableId pageId (return . (,()))

createPage
  :: DB
  -> TableId
  -> [Row] -- ^ Initial rows to populate
  -> Word16 -- ^ Initial size of populated rows
  -> STM PageId
createPage DB {..} tableId rows usedSpace = do
  tableData@TableData {..} <- Map.lookup tableId tableMap >>= maybe (throwSTM $ TableNotOpen tableId) return
  let
    pageId
      = pageCount tableHeader
    updatedTableData
      = tableData {tableHeader = TableHeader $ pageId + 1, dirtyHeader = True}
  Map.insert updatedTableData tableId tableMap
  Map.insert (MemPage (Page usedSpace rows) True) (tableId, pageId) pageMap
  return pageId

tableCounterFileName
  = (++ "/tableCounter")

fieldSpecMapFileName
  = (++ "/fieldSpecMap")

flushDB :: DB -> IO ()
flushDB DB {..} = do
  -- TODO: write tx counter
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
            hSeek handle AbsoluteSeek . toInteger $ tableHeaderSize + pageSize * (fromIntegral pageId)
            B.hPut handle . runPut . putPage $ page
          hFlush handle

    flushTableCounter
      = B.writeFile (tableCounterFileName dataDir) . encode

    flushFieldSpecMap
      = B.writeFile (fieldSpecMapFileName dataDir) . encode

beginTx :: DB -> STM TxId
beginTx DB {..} = do
  txId <- readTVar txCounter
  writeTVar txCounter (txId + 1)
  Map.insert newTx txId txMap
  return txId

commitTx :: DB -> TxId -> STM ()
commitTx DB {..} txId
  = Map.delete txId txMap

-- Undo all modifications by the transaction by looking at all pages
-- touched by it
--   - If a row has tMax = tx and tMin != tx then remove tMax
--   - If a row has tMin = tx then set tMax = tx (effectively deleting
--     it)
rollbackTx :: DB -> TxId -> IO ()
rollbackTx db@DB {..} txId = do
  Tx {..} <- atomically $ Map.lookup txId txMap >>= maybe (throwSTM $ InvalidTx txId) return
  -- TODO: update first those pages in memory, to avoid page table
  -- churn
  forM_ (Set.toList writtenPages) $ \(tableId, pageId) -> do
    alterPage db tableId pageId rollbackPage
  where
    rollbackPage (MemPage Page {..} isDirty) = do
      return . (,()) $ MemPage (Page usedSpace rows') (pageModified || isDirty)
      where
        (rows', pageModified)
          = foldr helper ([], False) rows

        helper row@Row {..} (rows, pageModified)
          | tmax == Just txId && tmin /= txId
              = (row { tmax = Nothing } : rows, True)
          | tmin == txId && tmax /= Just txId
              = (row { tmax = Just txId } : rows, True)
          | otherwise
              = (row : rows, pageModified)

-- Scan table, returning rows visible to the current transaction
scanTable :: DB -> TxId -> TableId -> IO [Row]
scanTable db@DB {..} txId tableId = do
  (pageCount, activeTxIds) <- atomically $ do
    TableHeader {..} <- Map.lookup tableId tableMap >>= maybe (throwSTM $ TableNotOpen tableId) (return . tableHeader)
    -- TODO: consider use of IntSet here
    activeTxIds <- fmap (Set.fromList . map fst) . toReverseList . Map.listT $ txMap
    return (pageCount, activeTxIds)
  -- TODO: iterate over pages in memory first to prevent churn
  fmap concat . sequence $ (flip map) [0 .. pageCount - 1] $ \pageId -> do
    MemPage Page {..} _ <- fetchPage db tableId pageId
    return $ filter (isRowVisible activeTxIds) rows
  where
    isRowVisible :: Set TxId -> Row -> Bool
    isRowVisible activeTxIds Row {..}
      = isVisible tmin && not (maybe False isVisible tmax)
      where
        -- Determine if a transaction's action is visible to the
        -- current transaction
        isVisible tx
          = tx == txId || tx < txId && not (Set.member tx activeTxIds)

splitRows
  :: Word16 -- ^ Free space in page
  -> Word16 -- ^ Size per row
  -> [[Field]]
  -> ([[Field]], [[Field]])
splitRows freeSpace rowSize
  = (map snd *** map snd) . span ((<= freeSpace) . fst) . zip (scanl1 (+) (repeat rowSize))

insertRows :: DB -> TxId -> TableId -> [[Field]] -> IO ()
insertRows db@DB {..} txId tableId rows = do
  (pageCount, fieldSpec) <- atomically $ do
    TableHeader {..} <- Map.lookup tableId tableMap >>= maybe (throwSTM $ TableNotOpen tableId) (return . tableHeader)
    fieldSpec <- Map.lookup tableId fieldSpecMap >>= maybe (throwSTM $ MissingFieldSpec tableId) return
    return (pageCount, fieldSpec)
  let
    pages
      = if pageCount > 0 then [0 .. pageCount - 1] else []
  helper fieldSpec pages rows
  where
    helper _ _ []
      = return ()

    helper fieldSpec [] rows = do
      -- Create pages and insert remaining rows
      createHelper rows
      where
        rowSize
          = getRowSize fieldSpec

        createHelper []
          = return ()
        createHelper rows = do
          rows' <- forM currentRows $ \fields -> do
            if validateFields fieldSpec fields
              then
                return $ Row txId Nothing fields
              else
                throwIO $ InvalidRow fieldSpec fields
          atomically $ createPage db tableId rows' ((fromIntegral . length) currentRows * rowSize)
          createHelper nextRows
          where
            (currentRows, nextRows)
              = splitRows pageSpace rowSize rows

    helper fieldSpec (pageId : pageIds) rows = do
      -- If empty space, add to page if possible. Then recurse on next
      -- pages and remaining rows
      (_, nextRows) <- alterPage db tableId pageId modPage
      helper fieldSpec pageIds nextRows
      where
        rowSize
          = getRowSize fieldSpec
        modPage memPage@(MemPage (Page usedSpace pageRows) _) = do
          let
            freeSpace
              = pageSpace - usedSpace
            (currentRows, nextRows)
              = splitRows freeSpace rowSize rows
          if null currentRows
            then
              return (memPage, nextRows)
            else do
              currentRows' <- forM currentRows $ \fields -> do
                if validateFields fieldSpec fields
                  then
                    return $ Row txId Nothing fields
                  else
                    throwSTM $ InvalidRow fieldSpec fields
              let
                newUsedSpace
                  = pageSpace + (fromIntegral . length) currentRows * rowSize
                newPage
                  = Page newUsedSpace (pageRows ++ currentRows')
                newMemPage
                  = MemPage newPage True
              return (newMemPage, nextRows)

-- -- Visit pages, starting with those in memory, writing rows to free
-- -- space in pages. When filter function provided, scan page and mark
-- -- as deleted any rows matching j
-- updateTable
--   :: DB
--   -> TxId
--   -> TableId
--   -> ? -- ^ Rows to insert
--   -> ? -- ^ Optional filter function to work out whether rows should be deleted
--   -> IO ()
-- updateTable
--   = undefined
