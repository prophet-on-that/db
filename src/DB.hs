{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}

module DB where

import qualified Data.ByteString as B
import qualified StmContainers.Map as Map
import qualified Control.Concurrent.STM.Lock as L
import GHC.Conc (atomically, TVar, newTVar, readTVar, writeTVar, throwSTM)
import System.IO (Handle, openBinaryFile, IOMode(..), hSeek, SeekMode(..))
import Data.Serialize (Serialize, decode)
import GHC.Generics (Generic)
import Control.Exception (Exception, throw)
import Data.Typeable (Typeable)

data DBException
  = PageDecodeError TableId PageId String
  | TableHeaderDecodeError TableId String
  | TableHeaderNotLoaded TableId
  deriving (Show, Typeable)

instance Exception DBException

data TableHeader = TableHeader
  { pageCount :: Int
  } deriving (Show, Generic)

instance Serialize TableHeader

-- ^ Size of table header in bytes
tableHeaderSize
  = 8

data Page = Page
  { usedSpace :: Int -- ^ Used bytes in pageData
  , pageData :: B.ByteString
  } deriving (Show, Generic)

instance Serialize Page

-- ^ The size of a table page in bytes, including its header
pageSize :: Int
pageSize
  = 1024

pageDataSize :: Int
pageDataSize
  = pageSize - 8

newPage :: Page
newPage
  = Page 0 $ B.replicate pageDataSize 0

data MemPage a = MemPage
  { page :: a
  , isDirty :: Bool
  } deriving (Show)

type TableId = Int

type PageId = Int

type PageMap = Map.Map (TableId, PageId) (MemPage Page)

type LockMap = Map.Map TableId L.Lock

type HandleMap = Map.Map TableId Handle

type HeaderMap = Map.Map TableId (MemPage TableHeader)

data DB = DB
  { dataDir :: String
  , tableCounter :: TVar Int
  , pageMap :: PageMap
  , lockMap :: LockMap
  , handleMap :: HandleMap
  , headerMap :: HeaderMap
  }

newDB :: IO DB
newDB
  = atomically $
          DB "tmp"
      <$> newTVar 0 -- TODO: should be read from disk
      <*> Map.new
      <*> Map.new
      <*> Map.new
      <*> Map.new

getTableFileName :: String -> TableId -> String
getTableFileName dirName tableId
  = dirName ++ "/" ++ show tableId

getPage :: DB -> TableId -> PageId -> IO (MemPage Page)
getPage DB {..} tableId pageId = do
  pageOrLock <- atomically $ do
    page <- Map.lookup (tableId, pageId) pageMap
    case page of
      Just page' ->
        return $ Left page'
      Nothing -> do
        -- If page not loaded, load from disk. This is
        -- mutually-exclusive per table.
        lock <- Map.lookup tableId lockMap
        case lock of
          Nothing -> do
            lock <- L.new
            Map.insert lock tableId lockMap
            return $ Right lock
          Just lock' ->
            return $ Right lock'
  case pageOrLock of
    Left page ->
      return page
    Right lock -> do
      L.with lock $ do
        -- Check to see whether page has now been loaded
        page <- atomically $ Map.lookup (tableId, pageId) pageMap
        case page of
          Just page' ->
            return page'
          Nothing -> do
            -- Load page from disk
            -- Get file handle
            let
              -- Get handle and load table header
              getHandle = do
                handle <- openBinaryFile (getTableFileName dataDir tableId) ReadWriteMode
                -- TOOD: set buffer mode of handle
                header <- decode <$> B.hGet handle tableHeaderSize
                case header of
                  Left err ->
                    throw $ TableHeaderDecodeError tableId err
                  Right header' -> do
                    atomically $ do
                      Map.insert handle tableId handleMap
                      Map.insert (MemPage header' False) tableId headerMap
                    return handle
            handle' <- atomically $ Map.lookup tableId handleMap
            handle <- maybe getHandle return handle'
            hSeek handle AbsoluteSeek . toInteger $ tableHeaderSize + pageSize * pageId
            page <- decode <$> B.hGet handle pageSize
            case page of
              Left err ->
                throw $ PageDecodeError tableId pageId err
              Right page' -> do
                -- TODO: evict once page size reaches limit
                let
                  memPage
                    = MemPage page' False
                atomically $ Map.insert memPage (tableId, pageId) pageMap
                return memPage

-- For a table, we need to know how any pages are currently stored in
-- the table. Store this in the table header, which is loaded when the
-- table is first opened (when handler acquired). This can then be
-- consulted/modified when adding a page to a table.

-- For each page, need to store the amount of free space (and later
-- position of rows to permit variable-length rows).

createTable :: DB -> IO TableId
createTable DB {..}
  = atomically $ do
      tableId <- readTVar tableCounter
      writeTVar tableCounter $ tableId + 1
      let
        header
          = TableHeader 0
      Map.insert (MemPage header True) tableId headerMap
      return tableId

createPage :: DB -> TableId -> IO PageId
createPage DB {..} tableId
  = atomically $ do
      header <- Map.lookup tableId headerMap
      case header of
        Nothing ->
          throwSTM $ TableHeaderNotLoaded tableId
        Just (MemPage (TableHeader {..}) _) -> do
          Map.insert (MemPage (TableHeader $ pageCount + 1) True) tableId headerMap
          Map.insert (MemPage newPage True) (tableId, pageCount) pageMap
          return pageCount
