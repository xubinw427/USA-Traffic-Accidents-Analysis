import os
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score, f1_score
import joblib


rand_seed = 233
params = {
    'max_depth': 6,
    'min_child_weight': 5,
    'learning_rate': 0.01,
    'n_estimators': 1000,
    'subsample': 0.6,
    'colsample_bytree': 1,
    'objective': 'binary:logistic',
    'gamma': 0.1,
    'lambda': 0.05,
    'alpha': 0.05,
    'random_state': rand_seed
}
round_flag = False
flag_2016 = True


def train_xgboost_model(X_train, y_train, X_test, y_test):
    global params
    model = xgb.XGBClassifier(**params)
    if not round_flag:
        model.fit(X_train, y_train)
    else:
        dtrain = xgb.DMatrix(X_train, label=y_train)
        dval = xgb.DMatrix(X_test, label=y_test)
        evals = [(dtrain, 'train'), (dval, 'eval')]
        model = xgb.train(params, dtrain, num_boost_round=8000, evals=evals)
    return model


def test_single_data_xgboost(model, data):
    probabilities = model.predict_proba([data])[0]
    predicted_class = model.predict([data])[0]
    return predicted_class, probabilities.tolist()


def load_data_raw(path, file):
    df = pd.read_csv(os.path.join(path, file))
    label_column = 'label'
    feature_columns = [col for col in df.columns if col != label_column]

    # 重新组织 DataFrame 以便每三行形成一个块
    num_samples = len(df) // 3
    # 确保 df 的行数可以被三整除
    df = df.iloc[:num_samples * 3]
    # 重塑 DataFrame 为三维数组 (num_samples, 3, num_features)
    features = df[feature_columns].values.reshape(num_samples, 3, -1)

    # 直接获取每个样本第三行的标签
    labels = df.iloc[2::3][label_column].values

    # 将数据重塑为 XGBoost 的输入格式
    xgboost_features = features.reshape(num_samples, -1)  # 展平成二维特征矩阵

    return xgboost_features, labels


def load_data(path, file):
    df = pd.read_csv(os.path.join(path, file))
    label_column = 'label'
    unique_columns = ['Start_Lat', 'Start_Lng', 'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']

    # 确定所有非标签列和唯一列
    non_label_columns = [col for col in df.columns if col != label_column]
    different_columns = [col for col in non_label_columns if col not in unique_columns]

    # 重新组织 DataFrame 以便每三行形成一个块
    num_samples = len(df) // 3
    # 确保 df 的行数可以被三整除
    df = df.iloc[:num_samples * 3]

    # 获取唯一的特征列的值
    unique_features = df.groupby(df.index // 3)[unique_columns].first().values

    # 获取不同的特征列的值（保留三行的值）
    different_features = df[different_columns].values.reshape(num_samples, -1)

    # 获取标签列的值（只保留一行）
    labels = df.iloc[2::3][label_column].values

    # 将数据重塑为 XGBoost 的输入格式
    # print(unique_features.shape)
    # print(different_features.shape)
    xgboost_features = np.hstack([unique_features, different_features])
    xgboost_features = xgboost_features.reshape(num_samples, -1)  # 展平成二维特征矩阵

    return xgboost_features, labels


if __name__ == "__main__":
    X_train, y_train = load_data('E:\\data', 'grouped_2016_train.csv')
    X_test, y_test = load_data('E:\\data', 'grouped_2016_test.csv')
    '''
    columns_list = ['Temperature', 'Humidity', 'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',
                    'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop',
                    'Start_Lat', 'Start_Lng', 'month', 'hour', 'weekday', 'Condition_Cloud', 'Condition_Cloudy',
                    'Condition_Drizzle', 'Condition_Dust', 'Condition_Fair', 'Condition_Fog', 'Condition_Hail',
                    'Condition_Haze', 'Condition_Mist', 'Condition_Mix', 'Condition_Nearby',
                    'Condition_Precipitation', 'Condition_Rain', 'Condition_Shower', 'Condition_Sleet',
                    'Condition_Smoke', 'Condition_Snow', 'Condition_Squalls', 'Condition_T-Storm',
                    'Condition_Thunder', 'Condition_Tornado', 'Condition_Vicinity', 'Condition_Whirlwinds',
                    'Condition_Windy', 'Condition_Grains'] + ['label']
    df = df.reindex(columns=columns_list, fill_value=0)
    df = df[sorted(df.columns)]'''

    if not round_flag:
        model = train_xgboost_model(X_train, y_train, X_test, y_test)
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)
    else:
        model = train_xgboost_model(X_train, y_train, X_test, y_test)
        dval = xgb.DMatrix(X_test, label=y_test)
        y_pred = model.predict(dval)
        import numpy as np
        y_pred = np.where(y_pred > 0.5, 1, 0)

    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    print("Accuracy:", accuracy)
    print("F1 score:", f1)
    # model.save_model('xgboost_model.json')
    joblib.dump(model, 'xgb_seq_model.joblib')
    # model.save_model('xgboost_model.model')

    #data_point = [5.1, 3.5, 1.4, 0.2]
    #predicted_class, probabilities = test_single_data_xgboost(model, data_point)


