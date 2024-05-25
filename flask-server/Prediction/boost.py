import os
import pandas as pd
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
    'n_estimators': 600,
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

def train_xgboost_model_tuning(X_train, y_train):
    # global params
    # model = xgb.XGBClassifier(**params)

    from sklearn.model_selection import GridSearchCV
    cv_params = {'n_estimators': [300, 400, 500, 600, 700, 800]}
    # cv_params = {'max_depth': [3, 4, 5, 6, 7, 8, 9, 10], 'min_child_weight': [1, 2, 3, 4, 5]}
    # cv_params = {'gamma': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]}
    # cv_params = {'subsample': [0.6, 0.7, 0.8, 0.9], 'colsample_bytree': [0.6, 0.7, 0.8, 0.9]}
    # cv_params = {'reg_alpha': [0.05, 0.1, 1, 1.5, 2], 'reg_lambda': [0.05, 0.1, 1, 1.5, 2]}
    other_params = {'learning_rate': 0.01, 'n_estimators': 'output_2016_tree.csv', 'max_depth': 3, 'min_child_weight': 5, 'seed': 0,
                    'subsample': 0.6, 'colsample_bytree': 0.9, 'gamma': 0.1, 'reg_alpha': 0.05, 'reg_lambda': 0.05}
    model = xgb.XGBClassifier(**other_params)
    optimized_GBM = GridSearchCV(estimator=model, param_grid=cv_params, scoring='f1', cv=5, verbose=1, n_jobs=2)
    optimized_GBM.fit(X_train, y_train)
    evalute_result = optimized_GBM.cv_results_
    print('每轮迭代运行结果:{0}'.format(evalute_result))
    print('参数的最佳取值：{0}'.format(optimized_GBM.best_params_))
    print('最佳模型得分:{0}'.format(optimized_GBM.best_score_))

    #model = xgb.XGBClassifier(**params)
    #model.fit(X_train, y_train)
    return model


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


if __name__ == "__main__":
    if flag_2016:
        # df = load_data('E:\\data', '2016_fake_data.csv', '2016_real_data.csv')
        df = pd.read_csv(os.path.join('E:\\data', 'output_2016_tree.csv'))
        #X = df.iloc[:, :-1].values
        #y = df.iloc[:, -1].values
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
        df = df[sorted(df.columns)]
        y = df['label'].values
        X = df.drop('label', axis=1).values

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=rand_seed)
    else:
        data_dir = 'E:\\data'
        csv_files = ['output_2017_tree.csv', 'output_2018_tree.csv', 'output_2019_tree.csv', 'output_2020_tree.csv', 'output_2021_tree.csv']
        dfs = []
        for file in csv_files:
            df = pd.read_csv(os.path.join(data_dir, file))
            dfs.append(df)
        merged_df = pd.concat(dfs, ignore_index=True).fillna(0)
        print(merged_df.isna().sum())
        df = pd.read_csv(os.path.join(data_dir, 'output_2016_tree.csv'))

        all_columns = set(merged_df.columns).union(set(df.columns))
        for col in all_columns:
            if col not in merged_df.columns:
                merged_df[col] = 0
            elif col not in df.columns:
                df[col] = 0

        merged_df = merged_df[sorted(all_columns)]
        df = df[sorted(all_columns)]

        y = df['label'].values
        X = df.drop('label', axis=1).values
        y_add = merged_df['label'].values
        X_add = merged_df.drop('label', axis=1).values
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=rand_seed)
        import numpy as np
        print(X_train.shape)
        print(X_add.shape)
        X_train = np.concatenate((X_train, X_add), axis=0)
        y_train = np.concatenate((y_train, y_add), axis=0)
        # y_train = np.vstack((y_train, y_add))

    # model = train_xgboost_model_tuning(X_train, y_train)
    # exit(0)
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
    joblib.dump(model, 'xgb_model.joblib')
    # model.save_model('xgboost_model.model')

    #data_point = [5.1, 3.5, 1.4, 0.2]
    #predicted_class, probabilities = test_single_data_xgboost(model, data_point)


