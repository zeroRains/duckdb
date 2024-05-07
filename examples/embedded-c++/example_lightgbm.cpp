#include <LightGBM/c_api.h>
#include <iostream>
#include <vector>

std::string predict(std::string data) {
	std::string pred_result = "";
	int temp;
	int p = 1;
	BoosterHandle handle;

	// load model
	temp = LGBM_BoosterCreateFromModelfile("/root/duckdb_test/test_raven/Credit_Card/creditcard_lgb_model.txt", &p,
	                                       &handle);
	std::cout << "load result value is " << temp << std::endl;

	// file data
	// const char* para = "None";
	// int res = LGBM_BoosterPredictForFile(handle, "test_data.csv", 0, C_API_PREDICT_NORMAL, 0, 1, para, "result");
	// std::cout << "file predict result is " << res << std::endl;
	int state;
	// row data
	int a[29 * 2] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	void *in_p = static_cast<void *>(a);

	std::vector<double> out(2, 0);
	double *out_result = static_cast<double *>(out.data());

	int64_t out_len;
	state = LGBM_BoosterPredictForMat(handle, in_p, C_API_DTYPE_FLOAT32, 2, 29, 1, C_API_PREDICT_NORMAL, 50, 1, "",
	                                  &out_len, out_result);
	std::cout << "row predict return is " << state << std::endl;
	std::cout << "row predict result size is " << out.size() << " value is " << out[0] << std::endl;
  	std::cout << "row predict result size is " << out.size() << " value is " << out[1] << std::endl;

	return pred_result;
	/*I know the above return statement is completely insignificant. But i wanted to use the loaded model to predict the
	 * data points further.*/
}

int main() {
	predict("hahaha");

	std::cout << "Ok complete!" << std::endl;
	return 0;
}