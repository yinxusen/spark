#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for Spark ML Python APIs.
"""
import array
import sys
if sys.version > '3':
    xrange = range
    basestring = str

try:
    import xmlrunner
except ImportError:
    xmlrunner = None

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from shutil import rmtree
import tempfile
import numpy as np
import inspect

from pyspark import keyword_only
from pyspark.ml import Estimator, Model, Pipeline, PipelineModel, Transformer
from pyspark.ml.classification import *
from pyspark.ml.clustering import *
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import *
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasMaxIter, HasInputCol, HasSeed
from pyspark.ml.recommendation import ALS
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor
from pyspark.ml.tuning import *
from pyspark.ml.wrapper import JavaParams
from pyspark.mllib.common import _java2py
from pyspark.mllib.linalg import Vectors, DenseVector, SparseVector
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import rand
from pyspark.sql.utils import IllegalArgumentException
from pyspark.storagelevel import *
from pyspark.tests import ReusedPySparkTestCase as PySparkTestCase


class SparkSessionTestCase(PySparkTestCase):
    @classmethod
    def setUpClass(cls):
        PySparkTestCase.setUpClass()
        cls.spark = SparkSession(cls.sc)

    @classmethod
    def tearDownClass(cls):
        PySparkTestCase.tearDownClass()
        cls.spark.stop()


class MockDataset(DataFrame):

    def __init__(self):
        self.index = 0


class HasFake(Params):

    def __init__(self):
        super(HasFake, self).__init__()
        self.fake = Param(self, "fake", "fake param")

    def getFake(self):
        return self.getOrDefault(self.fake)


class MockTransformer(Transformer, HasFake):

    def __init__(self):
        super(MockTransformer, self).__init__()
        self.dataset_index = None

    def _transform(self, dataset):
        self.dataset_index = dataset.index
        dataset.index += 1
        return dataset


class MockEstimator(Estimator, HasFake):

    def __init__(self):
        super(MockEstimator, self).__init__()
        self.dataset_index = None

    def _fit(self, dataset):
        self.dataset_index = dataset.index
        model = MockModel()
        self._copyValues(model)
        return model


class MockModel(MockTransformer, Model, HasFake):
    pass


class ParamTypeConversionTests(PySparkTestCase):
    """
    Test that param type conversion happens.
    """

    def test_int(self):
        lr = LogisticRegression(maxIter=5.0)
        self.assertEqual(lr.getMaxIter(), 5)
        self.assertTrue(type(lr.getMaxIter()) == int)
        self.assertRaises(TypeError, lambda: LogisticRegression(maxIter="notAnInt"))
        self.assertRaises(TypeError, lambda: LogisticRegression(maxIter=5.1))

    def test_float(self):
        lr = LogisticRegression(tol=1)
        self.assertEqual(lr.getTol(), 1.0)
        self.assertTrue(type(lr.getTol()) == float)
        self.assertRaises(TypeError, lambda: LogisticRegression(tol="notAFloat"))

    def test_vector(self):
        ewp = ElementwiseProduct(scalingVec=[1, 3])
        self.assertEqual(ewp.getScalingVec(), DenseVector([1.0, 3.0]))
        ewp = ElementwiseProduct(scalingVec=np.array([1.2, 3.4]))
        self.assertEqual(ewp.getScalingVec(), DenseVector([1.2, 3.4]))
        self.assertRaises(TypeError, lambda: ElementwiseProduct(scalingVec=["a", "b"]))

    def test_list(self):
        l = [0, 1]
        for lst_like in [l, np.array(l), DenseVector(l), SparseVector(len(l), range(len(l)), l),
                         array.array('l', l), xrange(2), tuple(l)]:
            converted = TypeConverters.toList(lst_like)
            self.assertEqual(type(converted), list)
            self.assertListEqual(converted, l)

    def test_list_int(self):
        for indices in [[1.0, 2.0], np.array([1.0, 2.0]), DenseVector([1.0, 2.0]),
                        SparseVector(2, {0: 1.0, 1: 2.0}), xrange(1, 3), (1.0, 2.0),
                        array.array('d', [1.0, 2.0])]:
            vs = VectorSlicer(indices=indices)
            self.assertListEqual(vs.getIndices(), [1, 2])
            self.assertTrue(all([type(v) == int for v in vs.getIndices()]))
        self.assertRaises(TypeError, lambda: VectorSlicer(indices=["a", "b"]))

    def test_list_float(self):
        b = Bucketizer(splits=[1, 4])
        self.assertEqual(b.getSplits(), [1.0, 4.0])
        self.assertTrue(all([type(v) == float for v in b.getSplits()]))
        self.assertRaises(TypeError, lambda: Bucketizer(splits=["a", 1.0]))

    def test_list_string(self):
        for labels in [np.array(['a', u'b']), ['a', u'b'], np.array(['a', 'b'])]:
            idx_to_string = IndexToString(labels=labels)
            self.assertListEqual(idx_to_string.getLabels(), ['a', 'b'])
        self.assertRaises(TypeError, lambda: IndexToString(labels=['a', 2]))

    def test_string(self):
        lr = LogisticRegression()
        for col in ['features', u'features', np.str_('features')]:
            lr.setFeaturesCol(col)
            self.assertEqual(lr.getFeaturesCol(), 'features')
        self.assertRaises(TypeError, lambda: LogisticRegression(featuresCol=2.3))

    def test_bool(self):
        self.assertRaises(TypeError, lambda: LogisticRegression(fitIntercept=1))
        self.assertRaises(TypeError, lambda: LogisticRegression(fitIntercept="false"))


class PipelineTests(PySparkTestCase):

    def test_pipeline(self):
        dataset = MockDataset()
        estimator0 = MockEstimator()
        transformer1 = MockTransformer()
        estimator2 = MockEstimator()
        transformer3 = MockTransformer()
        pipeline = Pipeline(stages=[estimator0, transformer1, estimator2, transformer3])
        pipeline_model = pipeline.fit(dataset, {estimator0.fake: 0, transformer1.fake: 1})
        model0, transformer1, model2, transformer3 = pipeline_model.stages
        self.assertEqual(0, model0.dataset_index)
        self.assertEqual(0, model0.getFake())
        self.assertEqual(1, transformer1.dataset_index)
        self.assertEqual(1, transformer1.getFake())
        self.assertEqual(2, dataset.index)
        self.assertIsNone(model2.dataset_index, "The last model shouldn't be called in fit.")
        self.assertIsNone(transformer3.dataset_index,
                          "The last transformer shouldn't be called in fit.")
        dataset = pipeline_model.transform(dataset)
        self.assertEqual(2, model0.dataset_index)
        self.assertEqual(3, transformer1.dataset_index)
        self.assertEqual(4, model2.dataset_index)
        self.assertEqual(5, transformer3.dataset_index)
        self.assertEqual(6, dataset.index)


class TestParams(HasMaxIter, HasInputCol, HasSeed):
    """
    A subclass of Params mixed with HasMaxIter, HasInputCol and HasSeed.
    """
    @keyword_only
    def __init__(self, seed=None):
        super(TestParams, self).__init__()
        self._setDefault(maxIter=10)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, seed=None):
        """
        setParams(self, seed=None)
        Sets params for this test.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


class OtherTestParams(HasMaxIter, HasInputCol, HasSeed):
    """
    A subclass of Params mixed with HasMaxIter, HasInputCol and HasSeed.
    """
    @keyword_only
    def __init__(self, seed=None):
        super(OtherTestParams, self).__init__()
        self._setDefault(maxIter=10)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, seed=None):
        """
        setParams(self, seed=None)
        Sets params for this test.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


class HasThrowableProperty(Params):

    def __init__(self):
        super(HasThrowableProperty, self).__init__()
        self.p = Param(self, "none", "empty param")

    @property
    def test_property(self):
        raise RuntimeError("Test property to raise error when invoked")


class ParamTests(PySparkTestCase):

    def test_copy_new_parent(self):
        testParams = TestParams()
        # Copying an instantiated param should fail
        with self.assertRaises(ValueError):
            testParams.maxIter._copy_new_parent(testParams)
        # Copying a dummy param should succeed
        TestParams.maxIter._copy_new_parent(testParams)
        maxIter = testParams.maxIter
        self.assertEqual(maxIter.name, "maxIter")
        self.assertEqual(maxIter.doc, "max number of iterations (>= 0).")
        self.assertTrue(maxIter.parent == testParams.uid)

    def test_param(self):
        testParams = TestParams()
        maxIter = testParams.maxIter
        self.assertEqual(maxIter.name, "maxIter")
        self.assertEqual(maxIter.doc, "max number of iterations (>= 0).")
        self.assertTrue(maxIter.parent == testParams.uid)

    def test_hasparam(self):
        testParams = TestParams()
        self.assertTrue(all([testParams.hasParam(p.name) for p in testParams.params]))
        self.assertFalse(testParams.hasParam("notAParameter"))

    def test_params(self):
        testParams = TestParams()
        maxIter = testParams.maxIter
        inputCol = testParams.inputCol
        seed = testParams.seed

        params = testParams.params
        self.assertEqual(params, [inputCol, maxIter, seed])

        self.assertTrue(testParams.hasParam(maxIter.name))
        self.assertTrue(testParams.hasDefault(maxIter))
        self.assertFalse(testParams.isSet(maxIter))
        self.assertTrue(testParams.isDefined(maxIter))
        self.assertEqual(testParams.getMaxIter(), 10)
        testParams.setMaxIter(100)
        self.assertTrue(testParams.isSet(maxIter))
        self.assertEqual(testParams.getMaxIter(), 100)

        self.assertTrue(testParams.hasParam(inputCol.name))
        self.assertFalse(testParams.hasDefault(inputCol))
        self.assertFalse(testParams.isSet(inputCol))
        self.assertFalse(testParams.isDefined(inputCol))
        with self.assertRaises(KeyError):
            testParams.getInputCol()

        # Since the default is normally random, set it to a known number for debug str
        testParams._setDefault(seed=41)
        testParams.setSeed(43)

        self.assertEqual(
            testParams.explainParams(),
            "\n".join(["inputCol: input column name. (undefined)",
                       "maxIter: max number of iterations (>= 0). (default: 10, current: 100)",
                       "seed: random seed. (default: 41, current: 43)"]))

    def test_kmeans_param(self):
        algo = KMeans()
        self.assertEqual(algo.getInitMode(), "k-means||")
        algo.setK(10)
        self.assertEqual(algo.getK(), 10)
        algo.setInitSteps(10)
        self.assertEqual(algo.getInitSteps(), 10)

    def test_hasseed(self):
        noSeedSpecd = TestParams()
        withSeedSpecd = TestParams(seed=42)
        other = OtherTestParams()
        # Check that we no longer use 42 as the magic number
        self.assertNotEqual(noSeedSpecd.getSeed(), 42)
        origSeed = noSeedSpecd.getSeed()
        # Check that we only compute the seed once
        self.assertEqual(noSeedSpecd.getSeed(), origSeed)
        # Check that a specified seed is honored
        self.assertEqual(withSeedSpecd.getSeed(), 42)
        # Check that a different class has a different seed
        self.assertNotEqual(other.getSeed(), noSeedSpecd.getSeed())

    def test_param_property_error(self):
        param_store = HasThrowableProperty()
        self.assertRaises(RuntimeError, lambda: param_store.test_property)
        params = param_store.params  # should not invoke the property 'test_property'
        self.assertEqual(len(params), 1)

    def test_word2vec_param(self):
        model = Word2Vec().setWindowSize(6)
        # Check windowSize is set properly
        self.assertEqual(model.getWindowSize(), 6)


class FeatureTests(SparkSessionTestCase):

    def test_binarizer(self):
        b0 = Binarizer()
        self.assertListEqual(b0.params, [b0.inputCol, b0.outputCol, b0.threshold])
        self.assertTrue(all([~b0.isSet(p) for p in b0.params]))
        self.assertTrue(b0.hasDefault(b0.threshold))
        self.assertEqual(b0.getThreshold(), 0.0)
        b0.setParams(inputCol="input", outputCol="output").setThreshold(1.0)
        self.assertTrue(all([b0.isSet(p) for p in b0.params]))
        self.assertEqual(b0.getThreshold(), 1.0)
        self.assertEqual(b0.getInputCol(), "input")
        self.assertEqual(b0.getOutputCol(), "output")

        b0c = b0.copy({b0.threshold: 2.0})
        self.assertEqual(b0c.uid, b0.uid)
        self.assertListEqual(b0c.params, b0.params)
        self.assertEqual(b0c.getThreshold(), 2.0)

        b1 = Binarizer(threshold=2.0, inputCol="input", outputCol="output")
        self.assertNotEqual(b1.uid, b0.uid)
        self.assertEqual(b1.getThreshold(), 2.0)
        self.assertEqual(b1.getInputCol(), "input")
        self.assertEqual(b1.getOutputCol(), "output")

    def test_idf(self):
        dataset = self.spark.createDataFrame([
            (DenseVector([1.0, 2.0]),),
            (DenseVector([0.0, 1.0]),),
            (DenseVector([3.0, 0.2]),)], ["tf"])
        idf0 = IDF(inputCol="tf")
        self.assertListEqual(idf0.params, [idf0.inputCol, idf0.minDocFreq, idf0.outputCol])
        idf0m = idf0.fit(dataset, {idf0.outputCol: "idf"})
        self.assertEqual(idf0m.uid, idf0.uid,
                         "Model should inherit the UID from its parent estimator.")
        output = idf0m.transform(dataset)
        self.assertIsNotNone(output.head().idf)

    def test_ngram(self):
        dataset = self.spark.createDataFrame([
            Row(input=["a", "b", "c", "d", "e"])])
        ngram0 = NGram(n=4, inputCol="input", outputCol="output")
        self.assertEqual(ngram0.getN(), 4)
        self.assertEqual(ngram0.getInputCol(), "input")
        self.assertEqual(ngram0.getOutputCol(), "output")
        transformedDF = ngram0.transform(dataset)
        self.assertEqual(transformedDF.head().output, ["a b c d", "b c d e"])

    def test_stopwordsremover(self):
        dataset = self.spark.createDataFrame([Row(input=["a", "panda"])])
        stopWordRemover = StopWordsRemover(inputCol="input", outputCol="output")
        # Default
        self.assertEqual(stopWordRemover.getInputCol(), "input")
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, ["panda"])
        self.assertEqual(type(stopWordRemover.getStopWords()), list)
        self.assertTrue(isinstance(stopWordRemover.getStopWords()[0], basestring))
        # Custom
        stopwords = ["panda"]
        stopWordRemover.setStopWords(stopwords)
        self.assertEqual(stopWordRemover.getInputCol(), "input")
        self.assertEqual(stopWordRemover.getStopWords(), stopwords)
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, ["a"])
        # with language selection
        stopwords = StopWordsRemover.loadDefaultStopWords("turkish")
        dataset = self.spark.createDataFrame([Row(input=["acaba", "ama", "biri"])])
        stopWordRemover.setStopWords(stopwords)
        self.assertEqual(stopWordRemover.getStopWords(), stopwords)
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, [])

    def test_count_vectorizer_with_binary(self):
        dataset = self.spark.createDataFrame([
            (0, "a a a b b c".split(' '), SparseVector(3, {0: 1.0, 1: 1.0, 2: 1.0}),),
            (1, "a a".split(' '), SparseVector(3, {0: 1.0}),),
            (2, "a b".split(' '), SparseVector(3, {0: 1.0, 1: 1.0}),),
            (3, "c".split(' '), SparseVector(3, {2: 1.0}),)], ["id", "words", "expected"])
        cv = CountVectorizer(binary=True, inputCol="words", outputCol="features")
        model = cv.fit(dataset)

        transformedList = model.transform(dataset).select("features", "expected").collect()

        for r in transformedList:
            feature, expected = r
            self.assertEqual(feature, expected)


class HasInducedError(Params):

    def __init__(self):
        super(HasInducedError, self).__init__()
        self.inducedError = Param(self, "inducedError",
                                  "Uniformly-distributed error added to feature")

    def getInducedError(self):
        return self.getOrDefault(self.inducedError)


class InducedErrorModel(Model, HasInducedError):

    def __init__(self):
        super(InducedErrorModel, self).__init__()

    def _transform(self, dataset):
        return dataset.withColumn("prediction",
                                  dataset.feature + (rand(0) * self.getInducedError()))


class InducedErrorEstimator(Estimator, HasInducedError):

    def __init__(self, inducedError=1.0):
        super(InducedErrorEstimator, self).__init__()
        self._set(inducedError=inducedError)

    def _fit(self, dataset):
        model = InducedErrorModel()
        self._copyValues(model)
        return model


class CrossValidatorTests(SparkSessionTestCase):

    def test_copy(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvCopied = cv.copy()
        self.assertEqual(cv.getEstimator().uid, cvCopied.getEstimator().uid)

        cvModel = cv.fit(dataset)
        cvModelCopied = cvModel.copy()
        for index in range(len(cvModel.avgMetrics)):
            self.assertTrue(abs(cvModel.avgMetrics[index] - cvModelCopied.avgMetrics[index])
                            < 0.0001)

    def test_fit_minimize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        bestModel = cvModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(0.0, bestModelMetric, "Best model has RMSE of 0")

    def test_fit_maximize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        bestModel = cvModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(1.0, bestModelMetric, "Best model has R-squared of 1")

    def test_save_load(self):
        # This tests saving and loading the trained model only.
        # Save/load for CrossValidator will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        lrModel = cvModel.bestModel

        cvModelPath = temp_path + "/cvModel"
        lrModel.save(cvModelPath)
        loadedLrModel = LogisticRegressionModel.load(cvModelPath)
        self.assertEqual(loadedLrModel.uid, lrModel.uid)
        self.assertEqual(loadedLrModel.intercept, lrModel.intercept)


class TrainValidationSplitTests(SparkSessionTestCase):

    def test_fit_minimize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        bestModel = tvsModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(0.0, bestModelMetric, "Best model has RMSE of 0")

    def test_fit_maximize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        bestModel = tvsModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(1.0, bestModelMetric, "Best model has R-squared of 1")

    def test_save_load(self):
        # This tests saving and loading the trained model only.
        # Save/load for TrainValidationSplit will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        lrModel = tvsModel.bestModel

        tvsModelPath = temp_path + "/tvsModel"
        lrModel.save(tvsModelPath)
        loadedLrModel = LogisticRegressionModel.load(tvsModelPath)
        self.assertEqual(loadedLrModel.uid, lrModel.uid)
        self.assertEqual(loadedLrModel.intercept, lrModel.intercept)


class PersistenceTest(SparkSessionTestCase):

    def test_linear_regression(self):
        lr = LinearRegression(maxIter=1)
        path = tempfile.mkdtemp()
        lr_path = path + "/lr"
        lr.save(lr_path)
        lr2 = LinearRegression.load(lr_path)
        self.assertEqual(lr.uid, lr2.uid)
        self.assertEqual(type(lr.uid), type(lr2.uid))
        self.assertEqual(lr2.uid, lr2.maxIter.parent,
                         "Loaded LinearRegression instance uid (%s) did not match Param's uid (%s)"
                         % (lr2.uid, lr2.maxIter.parent))
        self.assertEqual(lr._defaultParamMap[lr.maxIter], lr2._defaultParamMap[lr2.maxIter],
                         "Loaded LinearRegression instance default params did not match " +
                         "original defaults")
        try:
            rmtree(path)
        except OSError:
            pass

    def test_logistic_regression(self):
        lr = LogisticRegression(maxIter=1)
        path = tempfile.mkdtemp()
        lr_path = path + "/logreg"
        lr.save(lr_path)
        lr2 = LogisticRegression.load(lr_path)
        self.assertEqual(lr2.uid, lr2.maxIter.parent,
                         "Loaded LogisticRegression instance uid (%s) "
                         "did not match Param's uid (%s)"
                         % (lr2.uid, lr2.maxIter.parent))
        self.assertEqual(lr._defaultParamMap[lr.maxIter], lr2._defaultParamMap[lr2.maxIter],
                         "Loaded LogisticRegression instance default params did not match " +
                         "original defaults")
        try:
            rmtree(path)
        except OSError:
            pass

    def _compare_pipelines(self, m1, m2):
        """
        Compare 2 ML types, asserting that they are equivalent.
        This currently supports:
         - basic types
         - Pipeline, PipelineModel
        This checks:
         - uid
         - type
         - Param values and parents
        """
        self.assertEqual(m1.uid, m2.uid)
        self.assertEqual(type(m1), type(m2))
        if isinstance(m1, JavaParams):
            self.assertEqual(len(m1.params), len(m2.params))
            for p in m1.params:
                self.assertEqual(m1.getOrDefault(p), m2.getOrDefault(p))
                self.assertEqual(p.parent, m2.getParam(p.name).parent)
        elif isinstance(m1, Pipeline):
            self.assertEqual(len(m1.getStages()), len(m2.getStages()))
            for s1, s2 in zip(m1.getStages(), m2.getStages()):
                self._compare_pipelines(s1, s2)
        elif isinstance(m1, PipelineModel):
            self.assertEqual(len(m1.stages), len(m2.stages))
            for s1, s2 in zip(m1.stages, m2.stages):
                self._compare_pipelines(s1, s2)
        else:
            raise RuntimeError("_compare_pipelines does not yet support type: %s" % type(m1))

    def test_pipeline_persistence(self):
        """
        Pipeline[HashingTF, PCA]
        """
        temp_path = tempfile.mkdtemp()

        try:
            df = self.spark.createDataFrame([(["a", "b", "c"],), (["c", "d", "e"],)], ["words"])
            tf = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
            pca = PCA(k=2, inputCol="features", outputCol="pca_features")
            pl = Pipeline(stages=[tf, pca])
            model = pl.fit(df)

            pipeline_path = temp_path + "/pipeline"
            pl.save(pipeline_path)
            loaded_pipeline = Pipeline.load(pipeline_path)
            self._compare_pipelines(pl, loaded_pipeline)

            model_path = temp_path + "/pipeline-model"
            model.save(model_path)
            loaded_model = PipelineModel.load(model_path)
            self._compare_pipelines(model, loaded_model)
        finally:
            try:
                rmtree(temp_path)
            except OSError:
                pass

    def test_nested_pipeline_persistence(self):
        """
        Pipeline[HashingTF, Pipeline[PCA]]
        """
        temp_path = tempfile.mkdtemp()

        try:
            df = self.spark.createDataFrame([(["a", "b", "c"],), (["c", "d", "e"],)], ["words"])
            tf = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
            pca = PCA(k=2, inputCol="features", outputCol="pca_features")
            p0 = Pipeline(stages=[pca])
            pl = Pipeline(stages=[tf, p0])
            model = pl.fit(df)

            pipeline_path = temp_path + "/pipeline"
            pl.save(pipeline_path)
            loaded_pipeline = Pipeline.load(pipeline_path)
            self._compare_pipelines(pl, loaded_pipeline)

            model_path = temp_path + "/pipeline-model"
            model.save(model_path)
            loaded_model = PipelineModel.load(model_path)
            self._compare_pipelines(model, loaded_model)
        finally:
            try:
                rmtree(temp_path)
            except OSError:
                pass

    def test_decisiontree_classifier(self):
        dt = DecisionTreeClassifier(maxDepth=1)
        path = tempfile.mkdtemp()
        dtc_path = path + "/dtc"
        dt.save(dtc_path)
        dt2 = DecisionTreeClassifier.load(dtc_path)
        self.assertEqual(dt2.uid, dt2.maxDepth.parent,
                         "Loaded DecisionTreeClassifier instance uid (%s) "
                         "did not match Param's uid (%s)"
                         % (dt2.uid, dt2.maxDepth.parent))
        self.assertEqual(dt._defaultParamMap[dt.maxDepth], dt2._defaultParamMap[dt2.maxDepth],
                         "Loaded DecisionTreeClassifier instance default params did not match " +
                         "original defaults")
        try:
            rmtree(path)
        except OSError:
            pass

    def test_decisiontree_regressor(self):
        dt = DecisionTreeRegressor(maxDepth=1)
        path = tempfile.mkdtemp()
        dtr_path = path + "/dtr"
        dt.save(dtr_path)
        dt2 = DecisionTreeClassifier.load(dtr_path)
        self.assertEqual(dt2.uid, dt2.maxDepth.parent,
                         "Loaded DecisionTreeRegressor instance uid (%s) "
                         "did not match Param's uid (%s)"
                         % (dt2.uid, dt2.maxDepth.parent))
        self.assertEqual(dt._defaultParamMap[dt.maxDepth], dt2._defaultParamMap[dt2.maxDepth],
                         "Loaded DecisionTreeRegressor instance default params did not match " +
                         "original defaults")
        try:
            rmtree(path)
        except OSError:
            pass


class LDATest(SparkSessionTestCase):

    def _compare(self, m1, m2):
        """
        Temp method for comparing instances.
        TODO: Replace with generic implementation once SPARK-14706 is merged.
        """
        self.assertEqual(m1.uid, m2.uid)
        self.assertEqual(type(m1), type(m2))
        self.assertEqual(len(m1.params), len(m2.params))
        for p in m1.params:
            if m1.isDefined(p):
                self.assertEqual(m1.getOrDefault(p), m2.getOrDefault(p))
                self.assertEqual(p.parent, m2.getParam(p.name).parent)
        if isinstance(m1, LDAModel):
            self.assertEqual(m1.vocabSize(), m2.vocabSize())
            self.assertEqual(m1.topicsMatrix(), m2.topicsMatrix())

    def test_persistence(self):
        # Test save/load for LDA, LocalLDAModel, DistributedLDAModel.
        df = self.spark.createDataFrame([
            [1, Vectors.dense([0.0, 1.0])],
            [2, Vectors.sparse(2, {0: 1.0})],
        ], ["id", "features"])
        # Fit model
        lda = LDA(k=2, seed=1, optimizer="em")
        distributedModel = lda.fit(df)
        self.assertTrue(distributedModel.isDistributed())
        localModel = distributedModel.toLocal()
        self.assertFalse(localModel.isDistributed())
        # Define paths
        path = tempfile.mkdtemp()
        lda_path = path + "/lda"
        dist_model_path = path + "/distLDAModel"
        local_model_path = path + "/localLDAModel"
        # Test LDA
        lda.save(lda_path)
        lda2 = LDA.load(lda_path)
        self._compare(lda, lda2)
        # Test DistributedLDAModel
        distributedModel.save(dist_model_path)
        distributedModel2 = DistributedLDAModel.load(dist_model_path)
        self._compare(distributedModel, distributedModel2)
        # Test LocalLDAModel
        localModel.save(local_model_path)
        localModel2 = LocalLDAModel.load(local_model_path)
        self._compare(localModel, localModel2)
        # Clean up
        try:
            rmtree(path)
        except OSError:
            pass


class TrainingSummaryTest(SparkSessionTestCase):

    def test_linear_regression_summary(self):
        from pyspark.mllib.linalg import Vectors
        df = self.spark.createDataFrame([(1.0, 2.0, Vectors.dense(1.0)),
                                         (0.0, 2.0, Vectors.sparse(1, [], []))],
                                        ["label", "weight", "features"])
        lr = LinearRegression(maxIter=5, regParam=0.0, solver="normal", weightCol="weight",
                              fitIntercept=False)
        model = lr.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertGreater(s.totalIterations, 0)
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.predictionCol, "prediction")
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.featuresCol, "features")
        objHist = s.objectiveHistory
        self.assertTrue(isinstance(objHist, list) and isinstance(objHist[0], float))
        self.assertAlmostEqual(s.explainedVariance, 0.25, 2)
        self.assertAlmostEqual(s.meanAbsoluteError, 0.0)
        self.assertAlmostEqual(s.meanSquaredError, 0.0)
        self.assertAlmostEqual(s.rootMeanSquaredError, 0.0)
        self.assertAlmostEqual(s.r2, 1.0, 2)
        self.assertTrue(isinstance(s.residuals, DataFrame))
        self.assertEqual(s.numInstances, 2)
        devResiduals = s.devianceResiduals
        self.assertTrue(isinstance(devResiduals, list) and isinstance(devResiduals[0], float))
        coefStdErr = s.coefficientStandardErrors
        self.assertTrue(isinstance(coefStdErr, list) and isinstance(coefStdErr[0], float))
        tValues = s.tValues
        self.assertTrue(isinstance(tValues, list) and isinstance(tValues[0], float))
        pValues = s.pValues
        self.assertTrue(isinstance(pValues, list) and isinstance(pValues[0], float))
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.explainedVariance, s.explainedVariance)

    def test_logistic_regression_summary(self):
        from pyspark.mllib.linalg import Vectors
        df = self.spark.createDataFrame([(1.0, 2.0, Vectors.dense(1.0)),
                                         (0.0, 2.0, Vectors.sparse(1, [], []))],
                                        ["label", "weight", "features"])
        lr = LogisticRegression(maxIter=5, regParam=0.01, weightCol="weight", fitIntercept=False)
        model = lr.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.probabilityCol, "probability")
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.featuresCol, "features")
        objHist = s.objectiveHistory
        self.assertTrue(isinstance(objHist, list) and isinstance(objHist[0], float))
        self.assertGreater(s.totalIterations, 0)
        self.assertTrue(isinstance(s.roc, DataFrame))
        self.assertAlmostEqual(s.areaUnderROC, 1.0, 2)
        self.assertTrue(isinstance(s.pr, DataFrame))
        self.assertTrue(isinstance(s.fMeasureByThreshold, DataFrame))
        self.assertTrue(isinstance(s.precisionByThreshold, DataFrame))
        self.assertTrue(isinstance(s.recallByThreshold, DataFrame))
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.areaUnderROC, s.areaUnderROC)


class OneVsRestTests(SparkSessionTestCase):

    def test_copy(self):
        df = self.spark.createDataFrame([(0.0, Vectors.dense(1.0, 0.8)),
                                         (1.0, Vectors.sparse(2, [], [])),
                                         (2.0, Vectors.dense(0.5, 0.5))],
                                        ["label", "features"])
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr)
        ovr1 = ovr.copy({lr.maxIter: 10})
        self.assertEqual(ovr.getClassifier().getMaxIter(), 5)
        self.assertEqual(ovr1.getClassifier().getMaxIter(), 10)
        model = ovr.fit(df)
        model1 = model.copy({model.predictionCol: "indexed"})
        self.assertEqual(model1.getPredictionCol(), "indexed")

    def test_output_columns(self):
        df = self.spark.createDataFrame([(0.0, Vectors.dense(1.0, 0.8)),
                                         (1.0, Vectors.sparse(2, [], [])),
                                         (2.0, Vectors.dense(0.5, 0.5))],
                                        ["label", "features"])
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr)
        model = ovr.fit(df)
        output = model.transform(df)
        self.assertEqual(output.columns, ["label", "features", "prediction"])

    def test_save_load(self):
        temp_path = tempfile.mkdtemp()
        df = self.spark.createDataFrame([(0.0, Vectors.dense(1.0, 0.8)),
                                         (1.0, Vectors.sparse(2, [], [])),
                                         (2.0, Vectors.dense(0.5, 0.5))],
                                        ["label", "features"])
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr)
        model = ovr.fit(df)
        ovrPath = temp_path + "/ovr"
        ovr.save(ovrPath)
        loadedOvr = OneVsRest.load(ovrPath)
        self.assertEqual(loadedOvr.getFeaturesCol(), ovr.getFeaturesCol())
        self.assertEqual(loadedOvr.getLabelCol(), ovr.getLabelCol())
        self.assertEqual(loadedOvr.getClassifier().uid, ovr.getClassifier().uid)
        modelPath = temp_path + "/ovrModel"
        model.save(modelPath)
        loadedModel = OneVsRestModel.load(modelPath)
        for m, n in zip(model.models, loadedModel.models):
            self.assertEqual(m.uid, n.uid)


class HashingTFTest(SparkSessionTestCase):

    def test_apply_binary_term_freqs(self):

        df = self.spark.createDataFrame([(0, ["a", "a", "b", "c", "c", "c"])], ["id", "words"])
        n = 10
        hashingTF = HashingTF()
        hashingTF.setInputCol("words").setOutputCol("features").setNumFeatures(n).setBinary(True)
        output = hashingTF.transform(df)
        features = output.select("features").first().features.toArray()
        expected = Vectors.dense([1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]).toArray()
        for i in range(0, n):
            self.assertAlmostEqual(features[i], expected[i], 14, "Error at " + str(i) +
                                   ": expected " + str(expected[i]) + ", got " + str(features[i]))


class ALSTest(SparkSessionTestCase):

    def test_storage_levels(self):
        df = self.spark.createDataFrame(
            [(0, 0, 4.0), (0, 1, 2.0), (1, 1, 3.0), (1, 2, 4.0), (2, 1, 1.0), (2, 2, 5.0)],
            ["user", "item", "rating"])
        als = ALS().setMaxIter(1).setRank(1)
        # test default params
        als.fit(df)
        self.assertEqual(als.getIntermediateStorageLevel(), "MEMORY_AND_DISK")
        self.assertEqual(als._java_obj.getIntermediateStorageLevel(), "MEMORY_AND_DISK")
        self.assertEqual(als.getFinalStorageLevel(), "MEMORY_AND_DISK")
        self.assertEqual(als._java_obj.getFinalStorageLevel(), "MEMORY_AND_DISK")
        # test non-default params
        als.setIntermediateStorageLevel("MEMORY_ONLY_2")
        als.setFinalStorageLevel("DISK_ONLY")
        als.fit(df)
        self.assertEqual(als.getIntermediateStorageLevel(), "MEMORY_ONLY_2")
        self.assertEqual(als._java_obj.getIntermediateStorageLevel(), "MEMORY_ONLY_2")
        self.assertEqual(als.getFinalStorageLevel(), "DISK_ONLY")
        self.assertEqual(als._java_obj.getFinalStorageLevel(), "DISK_ONLY")


class DefaultValuesTests(PySparkTestCase):
    """
    Test :py:class:`JavaParams` classes to see if their default Param values match
    those in their Scala counterparts.
    """

    def check_params(self, py_stage):
        if not hasattr(py_stage, "_to_java"):
            return
        java_stage = py_stage._to_java()
        if java_stage is None:
            return
        for p in py_stage.params:
            java_param = java_stage.getParam(p.name)
            py_has_default = py_stage.hasDefault(p)
            java_has_default = java_stage.hasDefault(java_param)
            self.assertEqual(py_has_default, java_has_default,
                             "Default value mismatch of param %s for Params %s"
                             % (p.name, str(py_stage)))
            if py_has_default:
                if p.name == "seed":
                    return  # Random seeds between Spark and PySpark are different
                java_default =\
                    _java2py(self.sc, java_stage.clear(java_param).getOrDefault(java_param))
                py_stage._clear(p)
                py_default = py_stage.getOrDefault(p)
                self.assertEqual(java_default, py_default,
                                 "Java default %s != python default %s of param %s for Params %s"
                                 % (str(java_default), str(py_default), p.name, str(py_stage)))

    def test_java_params(self):
        import pyspark.ml.feature
        import pyspark.ml.classification
        import pyspark.ml.clustering
        import pyspark.ml.pipeline
        import pyspark.ml.recommendation
        import pyspark.ml.regression
        modules = [pyspark.ml.feature, pyspark.ml.classification, pyspark.ml.clustering,
                   pyspark.ml.pipeline, pyspark.ml.recommendation, pyspark.ml.regression]
        for module in modules:
            for name, cls in inspect.getmembers(module, inspect.isclass):
                if not name.endswith('Model') and issubclass(cls, JavaParams)\
                        and not inspect.isabstract(cls):
                    self.check_params(cls())


if __name__ == "__main__":
    from pyspark.ml.tests import *
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'))
    else:
        unittest.main()
