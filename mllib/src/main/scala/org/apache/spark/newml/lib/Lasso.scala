package org.apache.spark.newml.lib

import org.apache.spark.annotation.Experimental
import org.apache.spark.newml.linalg.Vector
import org.apache.spark.newml.model.VectorModel
import org.apache.spark.newml.optimization._
import org.apache.spark.newml.scheduler.StructuredScheduler
import org.apache.spark.rdd.RDD

/**
 * Train a regression model with L1-regularization using Stochastic Gradient Descent.
 * This solves the l1-regularized least squares regression formulation
 *          f(weights) = 1/n ||A weights-y||^2  + regParam ||weights||_1
 * Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
 * its corresponding right hand side label y.
 * See also the documentation for the precise formulation.
 */
case class Lasso private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var regParam: Double,
    private var miniBatchFraction: Double)
{

  private val gradient = new LeastSquaresGradient()
  private val updater = new L1Updater()
  private val scheduler = new StructuredScheduler()

  val optimizer = new GradientDescent(scheduler, gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)

  /**
   * Construct a Lasso object with default parameters: {stepSize: 1.0, numIterations: 100,
   * regParam: 0.01, miniBatchFraction: 1.0}.
   */
  def this() = this(1.0, 100, 0.01, 1.0)

  protected def createModel(weights: Vector, intercept: Double) = {
    new VectorModel(weights, intercept)
  }
}
