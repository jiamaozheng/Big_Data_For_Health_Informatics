import models
from sklearn.cross_validation import KFold, ShuffleSplit
from numpy import mean

import utils

#input: training data and corresponding labels
#output: accuracy, auc
def get_acc_auc_kfold(X,Y,k=5):
	#TODO:First get the train indices and test indices for each iteration
	#Then train the classifier accordingly
	#Report the mean accuracy and mean auc of all the folds

	#X_train.get_shape()[0]
	#print X.get_shape()[0]
	
	acc_arr = []
	auc_arr = []

	kfold = KFold(n=X.get_shape()[0], n_folds=k, random_state=545510477)
	for train_i, test_i in kfold:
		X_train, X_test = X[train_i], X[test_i]
		Y_train, Y_test = Y[train_i], Y[test_i]
		Y_pred = models.logistic_regression_pred(X_train, Y_train, X_test)
		acc, auc_, precision, recall, f1score = models.classification_metrics(Y_pred,Y_test)
		acc_arr.append(acc)
		auc_arr.append(auc_)
	return sum(acc_arr)/len(acc_arr),sum(auc_arr)/len(auc_arr)


#input: training data and corresponding labels
#output: accuracy, auc
def get_acc_auc_randomisedCV(X,Y,iterNo=5,test_percent=0.2):
	#TODO: First get the train indices and test indices for each iteration
	#Then train the classifier accordingly
	#Report the mean accuracy and mean auc of all the iterations

	acc_arr = []
	auc_arr = []

	shuffle_split = ShuffleSplit(n=X.get_shape()[0], n_iter=5, test_size=.2, random_state=545510477)
	for train_i, test_i in shuffle_split:
		X_train, X_test = X[train_i], X[test_i]
		Y_train, Y_test = Y[train_i], Y[test_i]
		Y_pred = models.logistic_regression_pred(X_train, Y_train, X_test)
		acc, auc_, precision, recall, f1score = models.classification_metrics(Y_pred,Y_test)
		acc_arr.append(acc)
		auc_arr.append(auc_)

	return sum(acc_arr)/len(acc_arr),sum(auc_arr)/len(auc_arr)


def main():
	X,Y = utils.get_data_from_svmlight("../deliverables/features_svmlight.train")
	print "Classifier: Logistic Regression__________"
	acc_k,auc_k = get_acc_auc_kfold(X,Y)
	print "Average Accuracy in KFold CV: "+str(acc_k)
	print "Average AUC in KFold CV: "+str(auc_k)
	acc_r,auc_r = get_acc_auc_randomisedCV(X,Y)
	print "Average Accuracy in Randomised CV: "+str(acc_r)
	print "Average AUC in Randomised CV: "+str(auc_r)

if __name__ == "__main__":
	main()

