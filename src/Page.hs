module Page where

import GHC.Generics (Generic)
import qualified Data.ByteString as B
import Data.Serialize (Serialize (..), putByteString, Get, Putter)
import Field (Field, FieldSpec, getField, fieldSpecSize, putField)
import Data.Word (Word32)

type TxId = Word32

-- ^ Transaction id representing Nothing in serialised row
txIdNone :: TxId
txIdNone = 0

-- ^ Minimum valid transaction id
txIdMin :: TxId
txIdMin = 1

data Row = Row
  { tmin :: TxId
  , tmax :: Maybe TxId
  , fields :: [Field]
  } deriving (Show)

rowSize :: FieldSpec -> Int
rowSize
  = (8 +) . fieldSpecSize

getTmax :: Get (Maybe TxId)
getTmax = do
  n <- get
  return $ if n == txIdNone then Nothing else Just n

getRow :: FieldSpec -> Get Row
getRow fieldSpec
  = Row <$> get
        <*> getTmax
        <*> mapM getField fieldSpec

putTmax :: Putter (Maybe TxId)
putTmax Nothing
  = put txIdNone
putTmax (Just n)
  = put n

putRow :: Putter Row
putRow Row {..} = do
  put tmin
  putTmax tmax
  mapM_ putField fields

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
