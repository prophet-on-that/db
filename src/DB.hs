{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}

module DB where

import qualified Data.ByteString as B
import qualified StmContainers.Map as Map
import qualified Control.Concurrent.STM.Lock as L
import GHC.Conc (atomically)
import System.IO (Handle, openBinaryFile, IOMode(..), hSeek, SeekMode(..))
import Data.Serialize (Serialize, decode)
import GHC.Generics (Generic)
import Control.Exception (Exception, throw)
import Data.Typeable (Typeable)

data DBException
  = PageDecodeError TableId PageId String
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
  { freeSpace :: Int
  , pageData :: B.ByteString
  } deriving (Show, Generic)

instance Serialize Page

-- ^ The size of a table page in bytes, including its header
pageSize :: Int
pageSize
  = 1024

data MemPage = MemPage
  { page :: Page
  , isDirty :: Bool
  } deriving (Show)

type TableId = Int

type PageId = Int

type PageMap = Map.Map (TableId, PageId) MemPage

type LockMap = Map.Map TableId L.Lock

type HandleMap = Map.Map TableId Handle

data DB = DB
  { pageMap :: PageMap
  , lockMap :: LockMap
  , handleMap :: HandleMap
  }

newDB :: IO DB
newDB
  = atomically $
      DB <$> Map.new <*> Map.new <*> Map.new

getPage :: DB -> TableId -> PageId -> IO MemPage
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
              getHandle = do
                handle <- openBinaryFile "test" ReadWriteMode -- TODO: make filename a function of table id
                -- TOOD: set buffer mode of handle
                atomically $ Map.insert handle tableId handleMap
                return handle
            handle' <- atomically $ Map.lookup tableId handleMap
            handle <- maybe getHandle return handle'
            hSeek handle AbsoluteSeek . toInteger $ pageSize * pageId
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

createPage :: TableId -> PageMap -> IO MemPage
createPage tableId pageMap = do
  undefined
