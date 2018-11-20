{-# LANGUAGE FlexibleInstances #-}
{-|
Module      :  Table
Description :  representing query results with nested lists
Copyright   :  (C) 2017-2018 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX
-}

module JudyGraph.Table where

import           Control.Monad(zipWithM)
import JudyGraph.Enum(Node32(..), Edge32(..), NodeAttribute (..), EdgeAttribute(..))


data NAttr = AllNodes
           | Label  [Int]
           | Nodes  [Node32]
           | Nodes2 [[Node32]]
           | Nodes3 [[[Node32]]]
           | Nodes4 [[[[Node32]]]]
           | Nodes5 [[[[[Node32]]]]]
           | Nodes6 [[[[[[Node32]]]]]]
           | Nodes7 [[[[[[[Node32]]]]]]]
           | Nodes8 [[[[[[[[Node32]]]]]]]]
           | Nodes9 [[[[[[[[[Node32]]]]]]]]] -- That should be enough
              deriving Show

data EAttr = Edges  [Edge32]
           | Edges2 [[Edge32]]
           | Edges3 [[[Edge32]]]
           | Edges4 [[[[Edge32]]]]
           | Edges5 [[[[[Edge32]]]]]
           | Edges6 [[[[[[Edge32]]]]]]
           | Edges7 [[[[[[[Edge32]]]]]]]
           | Edges8 [[[[[[[[Edge32]]]]]]]]
           | Edges9 [[[[[[[[[Edge32]]]]]]]]] -- That should be enough
              deriving Show


-- Keep structure and apply an IO-function to the lowest nesting of nodes
applyDeep :: (Node32 -> IO [Edge32]) -> NAttr -> IO EAttr
applyDeep f (Nodes ns)  = fmap Edges2 $ sequence (map f ns)
applyDeep f (Nodes2 ns) = fmap Edges3 $ sequence (map (sequence . (map f)) ns)
applyDeep f (Nodes3 ns) = fmap Edges4 $
  sequence (map (sequence . (map (sequence . (map f)))) ns)
applyDeep f (Nodes4 ns) = fmap Edges5 $
  sequence (map (sequence . (map (sequence . (map (sequence . (map f)))))) ns)
applyDeep f (Nodes5 ns) = fmap Edges6 $
  sequence (map (sequence .
           (map (sequence .
           (map (sequence . (map (sequence . (map f)))))))) ns)
applyDeep f (Nodes6 ns) = fmap Edges7 $
  sequence (map (sequence .
           (map (sequence .
           (map (sequence .
           (map (sequence . (map (sequence . (map f)))))))))) ns)
applyDeep f (Nodes7 ns) = fmap Edges8 $
  sequence (map (sequence .
           (map (sequence .
           (map (sequence .
           (map (sequence .
           (map (sequence . (map (sequence . (map f)))))))))))) ns)
applyDeep f (Nodes8 ns) = fmap Edges9 $
  sequence (map (sequence .
           (map (sequence .
           (map (sequence .
           (map (sequence .
           (map (sequence .
           (map (sequence . (map (sequence . (map f)))))))))))))) ns)


-- Keep structure and apply an IO-function to the lowest nesting of two lists
zipNAttr :: (NodeAttribute nl, EdgeAttribute el) =>
            graph nl el -> (Node32 -> [Edge32] -> IO [Node32])
            -> NAttr -- Node -- first 32 bit of several node-edges
            -> EAttr -- [EdgeAttr32] -- several second 32 bit
            -> IO NAttr -- [Node]) -- looked up nodes
zipNAttr graph f (Nodes  ns) (Edges2 es) = fmap Nodes2 $ zipWithM f ns es
zipNAttr graph f (Nodes2 ns) (Edges3 es) = fmap Nodes3 $ zipWithM (zipWithM f) ns es
zipNAttr graph f (Nodes3 ns) (Edges4 es) =
  fmap Nodes4 $ zipWithM (zipWithM (zipWithM f)) ns es
zipNAttr graph f (Nodes4 ns) (Edges5 es) =
  fmap Nodes5 $ zipWithM (zipWithM (zipWithM (zipWithM f))) ns es
zipNAttr graph f (Nodes5 ns) (Edges6 es) =
  fmap Nodes6 $ zipWithM (zipWithM (zipWithM (zipWithM (zipWithM f)))) ns es
zipNAttr graph f (Nodes6 ns) (Edges7 es) =
  fmap Nodes7 $ zipWithM (zipWithM (zipWithM (zipWithM (zipWithM (zipWithM f))))) ns es
zipNAttr graph f (Nodes7 ns) (Edges8 es) =
  fmap Nodes8 $
    zipWithM (zipWithM (zipWithM (zipWithM (zipWithM (zipWithM (zipWithM f)))))) ns es
zipNAttr graph f (Nodes8 ns) (Edges9 es) =
 fmap Nodes9 $
   zipWithM (zipWithM (zipWithM (zipWithM (zipWithM (zipWithM (zipWithM (zipWithM f)))))))
            ns es

class NestedLists a where
  toNAttr :: a -> NAttr

instance NestedLists [Node32] where toNAttr ns = Nodes ns
instance NestedLists [[Node32]] where toNAttr ns = Nodes2 ns
instance NestedLists [[[Node32]]] where toNAttr ns = Nodes3 ns
instance NestedLists [[[[Node32]]]] where toNAttr ns = Nodes4 ns
instance NestedLists [[[[[Node32]]]]] where toNAttr ns = Nodes5 ns
instance NestedLists [[[[[[Node32]]]]]] where toNAttr ns = Nodes6 ns
instance NestedLists [[[[[[[Node32]]]]]]] where toNAttr ns = Nodes7 ns


flatten (Nodes ns) = ns
flatten (Nodes2 ns) = concat ns
flatten (Nodes3 ns) = concat (concat ns)
flatten (Nodes4 ns) = concat (concat (concat ns))
flatten (Nodes5 ns) = concat (concat (concat (concat ns)))
flatten (Nodes6 ns) = concat (concat (concat (concat (concat ns))))
flatten (Nodes7 ns) = concat (concat (concat (concat (concat (concat ns)))))
flatten (Nodes8 ns) = concat (concat (concat (concat (concat (concat (concat ns))))))

flatten2 (Nodes ns) = [ns]
flatten2 (Nodes2 ns) = ns
flatten2 (Nodes3 ns) = concat ns
flatten2 (Nodes4 ns) = concat (concat ns)
flatten2 (Nodes5 ns) = concat (concat (concat ns))
flatten2 (Nodes6 ns) = concat (concat (concat (concat ns)))
flatten2 (Nodes7 ns) = concat (concat (concat (concat (concat ns))))
flatten2 (Nodes8 ns) = concat (concat (concat (concat (concat (concat ns)))))

flattenEs (Edges es)  = [es]
flattenEs (Edges2 es) = es
flattenEs (Edges3 es) = concat es
flattenEs (Edges4 es) = concat (concat es)
flattenEs (Edges5 es) = concat (concat (concat es))
flattenEs (Edges6 es) = concat (concat (concat (concat es)))
flattenEs (Edges7 es) = concat (concat (concat (concat (concat es))))
flattenEs (Edges8 es) = concat (concat (concat (concat (concat (concat es)))))

