# Spark MLlib 详解

## 目录

- [核心概念](#核心概念)
- [数据类型](#数据类型)
- [特征工程](#特征工程)
- [机器学习算法](#机器学习算法)
- [管道 API](#管道-api)
- [模型选择](#模型选择)

---

## 核心概念

### MLlib 简介

```
┌─────────────────────────────────────────────────────────────────┐
│                    MLlib 简介                                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Spark MLlib 是 Spark 的机器学习库:                              │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ✓ ML - DataFrame API (新 API，推荐)                     │   │
│  │  ✓ MLlib - RDD API (旧 API，逐步弃用)                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  vs 其他机器学习库:                                              │
│                                                                 │
│  | 特性        | MLlib       | Scikit-learn | TensorFlow |     │
│  |------------|-------------|--------------|-----------|     │
│  | 数据规模    | 大规模       | 中小规模     | 中大规模  |     │
│  | 分布式      | 原生支持     | 不支持       | 有限支持  |     │
│  | 迭代优化    | 高效         | 高效         | 高效      |     │
│  | 实时预测    | 支持         | 不支持       | 支持      |     │
│  | 算法丰富度  | 丰富         | 非常丰富     | 丰富      |     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 核心组件

```scala
// MLlib 核心组件

// 1. 数据类型
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.Matrix

// 2. ML (新 API)
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.recommendation._

// 3. MLlib (旧 RDD API)
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.optimization._
```

---

## 数据类型

### 局部向量

```scala
import org.apache.spark.mllib.linalg.{Vector, Vectors}

// 1. 密集向量
val dense1 = Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0)
val dense2 = Vectors.dense(Array(1.0, 2.0, 3.0, 4.0, 5.0))

// 2. 稀疏向量
// 格式: (维度, 索引数组, 值数组)
val sparse = Vectors.sparse(
    5,                    // 向量维度
    Array(0, 2, 4),      // 非零元素索引
    Array(1.0, 2.0, 3.0)  // 非零元素值
)
// 等价于: [1.0, 0.0, 2.0, 0.0, 3.0]

// 3. 访问向量元素
val v = Vectors.dense(1.0, 2.0, 3.0)
v(0)  // 1.0
v(1)  // 2.0

// 4. 向量操作
val v1 = Vectors.dense(1.0, 2.0)
val v2 = Vectors.dense(3.0, 4.0)
val sum = Vectors.dense(v1(0) + v2(0), v1(1) + v2(1))
```

### 局部矩阵

```scala
import org.apache.spark.mllib.linalg.{Matrix, Matrices}

// 1. 密集矩阵
// 格式: (行数, 列数, 存储方式, 数据数组)
val denseMatrix = Matrices.dense(
    3, 2,                    // 行数, 列数
    Array(
        1.0, 2.0, 3.0,      // 按列存储
        4.0, 5.0, 6.0
    )
)
/*
矩阵:
| 1.0  4.0 |
| 2.0  5.0 |
| 3.0  6.0 |
*/

// 2. 稀疏矩阵
val sparseMatrix = Matrices.sparse(
    3, 2,                    // 行数, 列数
    Array(0, 2, 4),         // 列起始索引
    Array(0, 1, 0, 1, 2),   // 行索引
    Array(1.0, 2.0, 4.0, 5.0, 3.0, 6.0)  // 非零值
)
```

### 分布式数据集

```scala
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

// 1. LabeledPoint - 标签向量
// 用于监督学习
val labeledPoint = LabeledPoint(
    label = 1.0,            // 标签
    features = Vectors.dense(1.0, 2.0, 3.0)
)

// 2. Rating - 评分
// 用于推荐系统
import org.apache.spark.mllib.regression.Rating
val rating = Rating(
    user = 1,               // 用户ID
    product = 100,          // 产品ID
    rating = 5.0            // 评分
)

// 3. 关联规则
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset

val transactions = Seq(
    Array("a", "b", "c"),
    Array("a", "b", "d"),
    Array("a", "c", "e")
)
val rdd = ssc.sparkContext.parallelize(transactions, 2)
```

---

## 特征工程

### 特征提取

```scala
import org.apache.spark.ml.feature._

// 1. HashingTF - 词频哈希
// 将文本转换为词频向量
val hashingTF = new HashingTF()
    .setInputCol("text")
    .setOutputCol("features")
    .setNumFeatures(10000)  // 哈希桶数量

val textData = Seq(
    "hello world",
    "hello spark",
    "world spark"
).toDF("text")

val featurizedData = hashingTF.transform(textData)

// 2. CountVectorizer - 词频向量
// 从文档集合提取词汇表
val countVectorizer = new CountVectorizer()
    .setInputCol("text")
    .setOutputCol("features")
    .setVocabSize(10000)    // 词汇表大小
    .setMinDF(2)            // 最小文档频率

// 3. TF-IDF
val tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words")

val tokenizedData = tokenizer.transform(textData)

val idf = new IDF()
    .setInputCol("features")
    .setOutputCol("idfFeatures")

val idfModel = idf.fit(featurizedData)
val tfidfData = idfModel.transform(featurizedData)

// 4. Word2Vec - 词向量
val word2Vec = new Word2Vec()
    .setInputCol("text")
    .setOutputCol("result")
    .setVectorSize(100)    // 词向量维度
    .setMinCount(1)        // 最小词频

val model = word2Vec.fit(textData)
val result = model.transform(textData)

// 5. FeatureHasher - 特征哈希
val hasher = new FeatureHasher()
    .setInputCol("raw")
    .setOutputCol("features")
    .setNumFeatures(1024)

val featureData = Seq(
    Map("feature1" -> 1.0, "feature2" -> 2.0),
    Map("feature3" -> 3.0)
).toDF("raw")

val hashedData = hasher.transform(featureData)
```

### 特征转换

```scala
// 1. StringIndexer - 字符串索引
// 将字符串标签转换为索引
val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")
    .fit(data)

val indexedData = indexer.transform(data)

// 2. IndexToString - 索引转字符串
val converter = new IndexToString()
    .setInputCol("categoryIndex")
    .setOutputCol("originalCategory")
    .setLabels(indexer.labels)

// 3. OneHotEncoder - 独热编码
val encoder = new OneHotEncoder()
    .setInputCol("categoryIndex")
    .setOutputCol("categoryVec")

val encodedData = encoder.transform(indexedData)

// 4. VectorIndexer - 向量索引
// 自动识别类别特征
val vectorIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(10)  // 超过此值视为连续特征

val indexerModel = vectorIndexer.fit(data)
val indexedFeatures = indexerModel.transform(data)

// 5. Normalizer - 归一化
val normalizer = new Normalizer()
    .setInputCol("features")
    .setOutputCol("normFeatures")
    .setP(1.0)  // L1 范数 (或 2.0 表示 L2 范数)

val normalizedData = normalizer.transform(data)

// 6. StandardScaler - 标准化
val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithMean(true)   // 是否中心化
    .setWithStd(true)    // 是否缩放

val scalerModel = scaler.fit(data)
val scaledData = scalerModel.transform(data)

// 7. MinMaxScaler - 最小最大缩放
val minMaxScaler = new MinMaxScaler()
    .setInputCol("features")
    .setOutputCol("minMaxFeatures")
    .setMin(0.0)
    .setMax(1.0)

val minMaxModel = minMaxScaler.fit(data)
val minMaxData = minMaxModel.transform(data)

// 8. Bucketizer - 分桶
val bucketizer = new Bucketizer()
    .setInputCol("feature")
    .setOutputCol("bucketedFeature")
    .setSplits(Array(0.0, 10.0, 20.0, 30.0, Double.PositiveInfinity))

val bucketedData = bucketizer.transform(dataWithOutliers)

// 9. QuantileDiscretizer - 分位数分桶
val discretizer = new QuantileDiscretizer()
    .setInputCol("feature")
    .setOutputCol("bucketedFeature")
    .setNumBuckets(5)  // 分桶数量

val discretizedData = discretizer.fit(data).transform(data)
```

### 特征选择

```scala
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors

// 1. VectorSlicer - 向量切片
// 从向量中选择指定索引的特征
val slicer = new VectorSlicer()
    .setInputCol("features")
    .setOutputCol("slicedFeatures")
    .setIndices(Array(0, 2))  // 选择索引 0 和 2

// 2. RFormula - R 公式语法
val formula = new RFormula()
    .setFormula("label ~ category + features")
    .setFeaturesCol("features")
    .setLabelCol("label")

val formulaData = formula.fit(data).transform(data)

// 3. ChiSqSelector - 卡方选择
// 基于卡方检验选择特征
val chiSelector = new ChiSqSelector()
    .setNumTopFeatures(10)
    .setFeaturesCol("features")
    .setLabelCol("label")
    .setOutputCol("selectedFeatures")

val selectorModel = chiSelector.fit(data)
val selectedData = selectorModel.transform(data)

// 4. PCA - 主成分分析
val pca = new PCA()
    .setInputCol("features")
    .setOutputCol("pcaFeatures")
    .setK(5)  // 保留5个主成分

val pcaModel = pca.fit(data)
val pcaData = pcaModel.transform(data)
```

---

## 机器学习算法

### 分类算法

```scala
import org.apache.spark.ml.classification._

// 1. 逻辑回归
val lr = new LogisticRegression()
    .setMaxIter(100)
    .setRegParam(0.01)
    .setElasticNetParam(0.8)

val lrModel = lr.fit(trainingData)
val predictions = lrModel.transform(testData)

// 2. 决策树
val dt = new DecisionTreeClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxDepth(5)
    .setMaxBins(32)
    .setMinInstancesPerNode(5)

val dtModel = dt.fit(trainingData)
val dtPredictions = dtModel.transform(testData)

// 3. 随机森林
val rf = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setNumTrees(100)
    .setMaxDepth(10)
    .setFeatureSubsetStrategy("auto")

val rfModel = rf.fit(trainingData)
val rfPredictions = rfModel.transform(testData)

// 4. 梯度提升树
val gbt = new GBTClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxIter(100)
    .setMaxDepth(5)
    .setStepSize(0.1)

val gbtModel = gbt.fit(trainingData)
val gbtPredictions = gbtModel.transform(testData)

// 5. 朴素贝叶斯
val nb = new NaiveBayes()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setSmoothing(1.0)

val nbModel = nb.fit(trainingData)
val nbPredictions = nbModel.transform(testData)

// 6. 多层感知机
val mlp = new MultilayerPerceptronClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setLayers(Array(4, 5, 3, 2))  // 输入层, 隐藏层, 输出层
    .setBlockSize(128)
    .setMaxIter(500)
    .setTol(1e-4)

// 7. 线性 SVM
val lsvm = new LinearSVC()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxIter(100)
    .setRegParam(0.1)

// 8. OneVsRest - 一对多分类
val ovr = new OneVsRest()
    .setClassifier(new LogisticRegression())
    .setLabelCol("label")
    .setFeaturesCol("features")

val ovrModel = ovr.fit(trainingData)
```

### 回归算法

```scala
import org.apache.spark.ml.regression._

// 1. 线性回归
val lr = new LinearRegression()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxIter(100)
    .setRegParam(0.01)
    .setElasticNetParam(0.8)

val lrModel = lr.fit(trainingData)
val lrPredictions = lrModel.transform(testData)
println(s"Coefficients: ${lrModel.coefficients}")
println(s"Intercept: ${lrModel.intercept}")

// 2. 广义线性回归
val glr = new GeneralizedLinearRegression()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setFamily("gaussian")
    .setLink("identity")
    .setMaxIter(100)
    .setRegParam(0.01)

// 3. 决策树回归
val dt = new DecisionTreeRegressor()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxDepth(5)

val dtModel = dt.fit(trainingData)

// 4. 随机森林回归
val rf = new RandomForestRegressor()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setNumTrees(100)
    .setMaxDepth(10)

val rfModel = rf.fit(trainingData)

// 5. 梯度提升树回归
val gbt = new GBTRegressor()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxIter(100)
    .setMaxDepth(5)
    .setStepSize(0.1)

val gbtModel = gbt.fit(trainingData)

// 6. 生存回归
val aft = new AFTSurvivalRegression()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setCensoringCol("censor")
    .setQuantileProbabilities(Array(0.1, 0.5, 0.9))

val aftModel = aft.fit(trainingData)
```

### 聚类算法

```scala
import org.apache.spark.ml.clustering._

// 1. K-Means
val kmeans = new KMeans()
    .setK(5)               // 聚类数
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
    .setMaxIter(100)
    .setInitMode("k-means||")
    .setInitSteps(5)
    .setTol(1e-4)

val kmeansModel = kmeans.fit(data)
val kmeansPredictions = kmeansModel.transform(data)

println(s"Cluster Centers: ${kmeansModel.clusterCenters.mkString(" ")}")

// 2. 高斯混合模型 (GMM)
val gmm = new GaussianMixture()
    .setK(3)               // 高斯成分数
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
    .setMaxIter(100)

val gmmModel = gmm.fit(data)
val gmmPredictions = gmmModel.transform(data)

// 3. 层次聚类 (Bisecting K-Means)
val bkm = new BisectingKMeans()
    .setK(5)
    .setFeaturesCol("features")
    .setMaxIter(100)
    .setMinDivisibleClusterSize(1.0)

val bkmModel = bkm.fit(data)

// 4. LDA (主题模型)
val lda = new LDA()
    .setK(10)              // 主题数
    .setFeaturesCol("features")
    .setMaxIter(20)
    .setOptimizer("online")
    .setDocConcentration(1.0)
    .setTopicConcentration(1.0)

val ldaModel = lda.fit(data)
val ldaTopics = ldaModel.describeTopics(10)
```

### 推荐算法

```scala
import org.apache.spark.ml.recommendation._

// 1. ALS (交替最小二乘)
val als = new ALS()
    .setRank(10)               // 隐因子数
    .setMaxIter(10)            // 最大迭代数
    .setRegParam(0.01)         // 正则化参数
    .setUserCol("userId")      // 用户列
    .setItemCol("movieId")     // 物品列
    .setRatingCol("rating")    // 评分列
    .setColdStartStrategy("drop")  // 冷启动策略

val alsModel = als.fit(trainingData)

// 2. 预测评分
val predictions = alsModel.transform(testData)

// 3. 为用户推荐物品
val userRecs = alsModel.recommendForAllUsers(10)

// 4. 为物品推荐用户
val itemRecs = alsModel.recommendForAllItems(10)

// 5. 为指定用户推荐物品
import org.apache.spark.sql.functions._
val users = trainingData.select(als.getUserCol).distinct()
val userSubset = users.limit(10)
val userSubsetRecs = alsModel.recommendForUserSubset(userSubset, 10)
```

### 频繁模式挖掘

```scala
import org.apache.spark.ml.fpm._

// 1. FPGrowth - 频繁模式挖掘
val fpgrowth = new FPGrowth()
    .setItemsCol("items")
    .setMinSupport(0.01)       // 最小支持度
    .setMinConfidence(0.5)     // 最小置信度

val fpgrowthModel = fpgrowth.fit(data)

// 获取频繁项集
val frequentItemsets = fpgrowthModel.freqItemsets

// 获取关联规则
val associationRules = fpgrowthModel.associationRules

// 2. PrefixSpan - 序列模式挖掘
import org.apache.spark.ml.fpm.PrefixSpan
val prefixSpan = new PrefixSpan()
    .setMinSupport(0.01)
    .setMaxPatternLength(10)

val prefixSpanModel = prefixSpan.fit(sequenceData)
val frequentSequences = prefixSpanModel.findFrequentSequentialPatterns()
```

---

## 管道 API

### Pipeline 创建

```scala
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{Tokenizer, HashingTF, StringIndexer}
import org.apache.spark.ml.classification.LogisticRegression

// 1. 定义各个阶段
val tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words")

val hashingTF = new HashingTF()
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("features")
    .setNumFeatures(1000)

val labelIndexer = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("indexedLabel")

val lr = new LogisticRegression()
    .setLabelCol("indexedLabel")
    .setFeaturesCol("features")
    .setMaxIter(10)

// 2. 创建管道
val pipeline = new Pipeline()
    .setStages(Array(
        tokenizer,
        hashingTF,
        labelIndexer,
        lr
    ))

// 3. 训练管道
val pipelineModel = pipeline.fit(trainingData)

// 4. 使用管道进行预测
val predictions = pipelineModel.transform(testData)

// 5. 保存管道模型
pipelineModel.save("/path/to/pipelineModel")

// 6. 加载管道模型
val loadedModel = PipelineModel.load("/path/to/pipelineModel")
```

### 交叉验证

```scala
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

// 1. 定义参数网格
val paramGrid = new ParamGridBuilder()
    .addGrid(hashingTF.numFeatures, Array(500, 1000, 2000))
    .addGrid(lr.regParam, Array(0.01, 0.1, 1.0))
    .addGrid(lr.maxIter, Array(50, 100))
    .build()

// 2. 定义评估器
val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("indexedLabel")
    .setMetricName("areaUnderROC")

// 3. 创建交叉验证器
val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEstimatorParamMaps(paramGrid)
    .setEvaluator(evaluator)
    .setNumFolds(5)  // 5 折交叉验证
    .setParallelism(2)

// 4. 执行交叉验证
val cvModel = cv.fit(trainingData)

// 5. 获取最佳模型
val bestModel = cvModel.bestModel

// 6. 获取评估结果
val avgMetrics = cvModel.avgMetrics
```

### 训练验证分割

```scala
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.RegressionEvaluator

// 1. 定义评估器
val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")

// 2. 创建训练验证分割
val trainValidationSplit = new TrainValidationSplit()
    .setEstimator(pipeline)
    .setEstimatorParamMaps(paramGrid)
    .setEvaluator(evaluator)
    .setTrainRatio(0.8)  // 80% 训练集
    .setParallelism(2)

// 3. 执行训练验证分割
val tvModel = trainValidationSplit.fit(trainingData)

// 4. 评估测试集
val testPredictions = tvModel.transform(testData)
val rmse = evaluator.evaluate(testPredictions)
```

---

## 模型选择

### 模型评估

```scala
import org.apache.spark.ml.evaluation._

// 1. 分类评估器
val binaryEvaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("rawPrediction")
    .setMetricName("areaUnderROC")

val binaryAUROC = binaryEvaluator.evaluate(predictions)

val multiclassEvaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

val accuracy = multiclassEvaluator.evaluate(predictions)

// 2. 回归评估器
val regressionEvaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")  // RMSE, mse, mae, r2

val rmse = regressionEvaluator.evaluate(predictions)

// 3. 聚类评估器
import org.apache.spark.ml.clustering.ClusteringEvaluator

val clusteringEvaluator = new ClusteringEvaluator()
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
    .setMetricName("silhouette")  // 轮廓系数

val silhouette = clusteringEvaluator.evaluate(predictions)
```

### 模型持久化

```scala
// 1. 保存单个模型
val model = lr.fit(trainingData)
model.save("/path/to/model")

// 2. 加载模型
val loadedModel = LogisticRegressionModel.load("/path/to/model")

// 3. 保存管道
pipeline.save("/path/to/pipeline")

// 4. 加载管道
val loadedPipeline = Pipeline.load("/path/to/pipeline")

// 5. 保存管道模型
pipelineModel.save("/path/to/pipelineModel")

// 6. 加载管道模型
val loadedPipelineModel = PipelineModel.load("/path/to/pipelineModel")
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | Spark 架构详解 |
| [02-spark-sql.md](02-spark-sql.md) | Spark SQL 详解 |
| [03-spark-streaming.md](03-spark-streaming.md) | Spark Streaming 详解 |
| [05-optimization.md](05-optimization.md) | 性能优化指南 |
| [README.md](README.md) | 索引文档 |
