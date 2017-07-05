package org.apache.spark.bdd

import scala.collection.Iterator

/**
 * Created by ali on 4/16/15.
 */
abstract class BDAbstractIterator[+T] extends Iterator[T]