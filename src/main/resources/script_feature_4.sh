echo "Script Feature 4 Start...!"
hadoop fs -rm filmNames.item
hadoop fs -rm u.data
hadoop fs -rm -r feature_4
gsutil cp -r gs://dataproc-926b27f7-162c-44a3-913e-1ee0390d33eb-asia-northeast2/feature_4 .
cd feature_4
hadoop fs -put filmNames.item
hadoop fs -put u.data
hadoop jar feature_4.jar u.data filmNames.item "Kolya (1996)" 10 0.97 5 feature_4
echo "Script Feature 4 Finish Processing...!"
echo "---------------- RESULT---------------"
hadoop fs -cat ./feature_4/congruentFilmsOut/*
echo "Script Feature 4 End...!"