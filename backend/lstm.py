import os
import numpy as np
import pandas as pd
import torch
from torch.utils.data import TensorDataset, DataLoader
from torch import nn, optim
import datetime
import torch.nn.functional as F


class LSTMClassifier(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_layers=1):
        super(LSTMClassifier, self).__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers=num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        lstm_last_output = lstm_out[:, -1, :]  # 取 LSTM 输出序列的最后一个时间步的输出
        lstm_last_output = F.relu(lstm_last_output)  # 在 LSTM 输出后应用 ReLU 激活函数
        out = self.fc(lstm_last_output)
        out = torch.sigmoid(out)  # 在线性层之前应用 sigmoid 激活函数
        return out


class LSTMClassifier_old(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_layers=1):
        super(LSTMClassifier_old, self).__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers=num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x):
        _, (h_n, _) = self.lstm(x)
        out = self.fc(h_n[-1])
        return torch.sigmoid(out)


def train_lstm_model(model, train_loader, criterion, optimizer, num_epochs=20, eval_loader=None):
    for epoch in range(num_epochs):
        model.train()
        start_time = datetime.datetime.now()  # 记录每个epoch开始的时间

        for inputs, labels in train_loader:
            outputs = model(inputs).squeeze()  # 确保输出尺寸匹配
            loss = criterion(outputs, labels.float())  # BCELoss需要输入和标签同为float

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        end_time = datetime.datetime.now()  # 记录每个epoch结束的时间
        duration = end_time - start_time  # 计算持续时间

        print(f'Epoch {epoch + 1}/{num_epochs}, Loss: {loss.item():.4f}, Time: {duration}')

        if eval_loader and (epoch + 1) % 5 == 0:
            evaluate_model(model, eval_loader, criterion)


def evaluate_model(model, eval_loader, criterion):
    model.eval()
    correct = 0
    total = 0
    total_loss = 0
    with torch.no_grad():
        for inputs, labels in eval_loader:
            outputs = model(inputs).squeeze()
            loss = criterion(outputs, labels.float())
            total_loss += loss.item()

            # 计算准确率
            predicted = (outputs >= 0.5).float()  # 使用0.5作为分类阈值
            total += labels.size(0)
            correct += (predicted == labels).sum().item()

    avg_loss = total_loss / total
    accuracy = correct / total
    print(f'Test Loss: {avg_loss:.4f}, Accuracy: {accuracy:.4f}')


def to_dataLoader(path, file):
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

    # 转换为 PyTorch 的 Tensor
    features_tensor = torch.tensor(features, dtype=torch.float32)
    labels_tensor = torch.tensor(labels, dtype=torch.long)

    # 创建 TensorDataset 和 DataLoader
    dataset = TensorDataset(features_tensor, labels_tensor)
    loader = DataLoader(dataset, batch_size=32, shuffle=True)
    return loader, features.shape[2]  # 返回特征维数


if __name__ == "__main__":
    train_loader, M = to_dataLoader('E:\\data', 'grouped_2016_train.csv')
    test_loader, _ = to_dataLoader('E:\\data', 'grouped_2016_test.csv')
    print("Data loading done.")

    model = LSTMClassifier(input_dim=M, hidden_dim=50)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.0001)

    train_lstm_model(model, train_loader, criterion, optimizer, num_epochs=20, eval_loader=test_loader)
