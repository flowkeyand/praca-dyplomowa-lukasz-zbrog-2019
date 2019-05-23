echo "Script Feature 3 Start...!"
hadoop fs -rm filmNames.item
hadoop fs -rm u.data
hadoop fs -rm -r feature_3
gsutil cp -r gs://dataproc-926b27f7-162c-44a3-913e-1ee0390d33eb-asia-northeast2/feature_3 .
cd feature_3
hadoop fs -put filmNames.item
hadoop fs -put u.data
hadoop jar feature_3.jar u.data filmNames.item 420 3.58 10 feature_3
echo "Script Feature 3 Finish Processing...!"
echo "---------------- RESULT---------------"
echo "MOVIE NAME --- BAYES ESTIMATOR --- NUMBER OF REVIEWS"
hadoop fs -cat ./feature_3/bayesEstimatorOut/*
echo "Script Feature 3 End...!"