# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import sys
from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
import dask.dataframe as dd
from sagemaker_training import environment

import utils

MODEL_FILENAME = "model.ubj"


def _train(args):
    # Load all SageMaker environment variables
    env = environment.Environment()

    # Load raw input
    train_dir = Path(env.channel_input_dirs["training"])

    print(f"Loading data from {str(train_dir)}, files: {[str(f) for f in train_dir.iterdir()]}")

    df = dd.read_csv(str(train_dir / "*.csv")).compute()
    # df = dd.read_csv('s3://{EXT_TRAINING_DATA_BUCKET}/df_train/snapshot_date=2022-02-01-2022-03-02/*.csv').compute()

    print(f"Preparing training data")

    # Prepare training data
    label_cols = ['use_ssd', 'use_ssd_rush', 'switch_ssd', 'switch_ssd_rush', 'pay_ssd', 'pay_ssd_rush',
                  'pay_ssd_rush_next', 'pay_ssd_rush_next_1dc']

    cat_cols = ['first_non_default_selected_ship_option_group_in_60_days',
                'last_non_default_selected_ship_option_group_in_60_days']

    for col in cat_cols:
        df[col] = df[col].astype("category")

    SEED = 2
    df_train, df_test = train_test_split(df, test_size=0.5, random_state=SEED)
    df_tr, df_val = train_test_split(df_train, test_size=0.3, random_state=SEED)
    non_feature_cols = ['customer_id'] + label_cols + ['exp_ttl', 'shipping_cost', 'cond_sum']
    feature_cols = [x for x in df_tr.columns if x not in non_feature_cols]
    df_tr_x = df_tr[feature_cols].copy()
    df_val_x = df_val[feature_cols].copy()
    df_test_x = df_test[feature_cols].copy()

    # ignore some warnings for demo purposes
    from warnings import simplefilter
    simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
    simplefilter(action='ignore', category=FutureWarning)

    # hyper parameters
    # xgb_params = {
    #     'booster' : 'gbtree',
    #     #'eta': 0.01,
    #     'eta': 0.1,
    #     #'gamma' : 1.0 ,
    #     'gamma' : 0 ,
    #     'max_depth':6,
    #     'objective': "binary:logistic",
    #     # 'nthread':48,
    #     'eval_metric': "auc"
    # }
    xgb_params = env.hyperparameters

    xgbTest = xgb.DMatrix(df_test_x, enable_categorical=True)
    label = "pay_ssd_rush_next"

    print(f"Start training using label column: {label}")

    df_tr_y = df_tr[label].copy()
    df_val_y = df_val[label].copy()
    df_test_y = df_test[label].copy()
    xgbTrain = xgb.DMatrix(df_tr_x, label=df_tr_y, enable_categorical=True)
    xgbVal = xgb.DMatrix(df_val_x, label=df_val_y, enable_categorical=True)

    evallist = [(xgbTrain, 'train'), (xgbVal, 'val')]
    eval_res = {}
    model = xgb.train(xgb_params, xgbTrain, evals=evallist, num_boost_round=100, evals_result=eval_res, verbose_eval=10)

    print(f"Finished training, model = {model}, eval_res = {eval_res}")

    del xgbTrain
    del xgbVal

    _save_model(model, env.model_dir)


def _save_model(model, model_dir):
    print(f"Saving model to {model_dir}/{MODEL_FILENAME}")
    model.save_model(os.path.join(model_dir, MODEL_FILENAME))


def model_fn(model_dir):
    """
    This is a method defined in inference handler.
    Check https://github.com/aws/sagemaker-inference-toolkit for all inference handler methods.

    This method overrides the default `model_fn` defined in SageMaker prebuilt containers.
    We reuse other implementations (input_fn, predict_fn, output_fn) defined in SageMaker prebuilt containers (i.e. XGBoost container).

    Here we use the recommended way in https://docs.aws.amazon.com/sagemaker/latest/dg/xgboost.html#InputOutput-XGBoost
    to save and load the model.
    """

    print(f"Loading model from {model_dir}/{MODEL_FILENAME}")

    model = xgb.Booster()
    model.load_model(os.path.join(model_dir, MODEL_FILENAME))

    return model


def input_fn(input_data, content_type):
    """
    Take request data and de-serializes the data into an object for prediction.
    When an InvokeEndpoint operation is made against an Endpoint running SageMaker model server,
    the model server receives two pieces of information:
        - The request Content-Type, for example "application/json"
        - The request data, which is at most 5 MB (5 * 1024 * 1024 bytes) in size.
    The input_fn is responsible to take the request data and pre-process it before prediction.

    NOTICE: For CSV data, we MUST strip the leading and trailing newline characters before splitting by newlines.
    Otherwise, there will exist empty rows after splitting.

    Args:
        input_data (obj): the request data serialized in the content_type format.
        content_type (str): the request Content-Type.
    Returns:
        (obj): data ready for prediction. For XGBoost, this defaults to DMatrix.
    """

    print(f"content_type={content_type}")

    return utils.csv_to_dmatrix(input_data, float)


if __name__ == '__main__':
    _train(sys.argv)