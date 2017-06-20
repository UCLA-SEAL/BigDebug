package org.apache.spark.bdd

import scala.collection.Iterator

/**
 * Created by ali on 4/16/15.
 */
abstract class BDDAbstractIterator[+T] extends Iterator[T]