import os
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import pandas as pd
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer


rand_seed = 233
epochs = 10
lr_base = 0.001
batch_size = 32


class MLP(nn.Module):
    def __init__(self, input_size):
        super(MLP, self).__init__()
        self.layer1 = nn.Linear(input_size, 128)
        self.bn1 = nn.BatchNorm1d(128)
        self.dropout = nn.Dropout(0.1)
        self.layer2 = nn.Linear(128, 64)
        self.layer3 = nn.Linear(64, 2)

    def forward(self, x):
        x = F.leaky_relu(self.bn1(self.layer1(x)))
        x = self.dropout(x)
        x = F.leaky_relu(self.layer2(x))
        x = self.layer3(x)
        return x


class MLP2(nn.Module):
    def __init__(self, input_size):
        super(MLP2, self).__init__()
        self.layer1 = nn.Linear(input_size, 128)
        self.bn1 = nn.BatchNorm1d(128)
        self.layer2 = nn.Linear(128, 64)
        self.bn2 = nn.BatchNorm1d(64)
        self.dropout = nn.Dropout(0.1)
        self.layer3 = nn.Linear(64, 32)
        self.layer4 = nn.Linear(32, 2)

    def forward(self, x):
        x = F.leaky_relu(self.bn1(self.layer1(x)))
        x = F.leaky_relu(self.bn2(self.layer2(x)))
        x = self.dropout(x)
        x = F.leaky_relu(self.layer3(x))
        x = self.layer4(x)
        return x


class TimeSeriesMLP(nn.Module):
    def __init__(self, input_size, seq_length=3):
        super(TimeSeriesMLP, self).__init__()
        self.conv1 = nn.Conv1d(in_channels=input_size, out_channels=128, kernel_size=3, padding=1)
        self.bn1 = nn.BatchNorm1d(128)
        self.conv2 = nn.Conv1d(in_channels=128, out_channels=64, kernel_size=3, padding=1)
        self.bn2 = nn.BatchNorm1d(64)
        self.dropout = nn.Dropout(0.1)
        # Flatten the output for the fully connected layer
        self.fc_input_size = 64 * seq_length
        self.fc1 = nn.Linear(self.fc_input_size, 32)
        self.fc2 = nn.Linear(32, 2)

    def forward(self, x):
        # Expecting input of shape (batch_size, input_size, seq_length)
        # where input_size is the number of features per time step
        x = F.leaky_relu(self.bn1(self.conv1(x)))
        x = F.leaky_relu(self.bn2(self.conv2(x)))
        x = self.dropout(x)
        # Flatten the output for the fully connected layer
        x = x.view(-1, self.fc_input_size)
        x = F.leaky_relu(self.fc1(x))
        x = self.fc2(x)
        return x


def train(model, train_loader, criterion, optimizer, print_freq=1000):
    model.train()
    total_loss = 0
    iter_loss = 0
    for i, (inputs, labels) in enumerate(train_loader):
        inputs, labels = inputs.to(device), labels.to(device)
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
        iter_loss += loss.item()

        if (i + 1) % print_freq == 0:
            average_loss = iter_loss / print_freq
            print(f'Iteration [{i + 1}/{len(train_loader)}], Training Loss: {average_loss:.4f}')
            iter_loss = 0  # 重置损失累加器

    average_loss = total_loss / len(train_loader)
    print(f'Final Training Loss: {average_loss}')


def test(model, test_loader):
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for inputs, labels in test_loader:
            inputs, labels = inputs.to(device), labels.to(device)
            outputs = model(inputs)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()
    accuracy = 100 * correct / total
    print(f'Accuracy on test set: {accuracy}%')
    return accuracy


def test_single_data(model, data):
    model.eval()
    with torch.no_grad():
        input_tensor = torch.FloatTensor(data).unsqueeze(0).to(device)
        output = model(input_tensor)
        probabilities = torch.softmax(output, dim=1) # prob
        _, predicted = torch.max(output.data, 1) # pred result
    return predicted.item(), probabilities.squeeze().tolist()


def load_model(model_path):
    model = torch.load(model_path)
    return model


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f'Using {device} device.')

if __name__ == "__main__":
    # df = load_data('E:\\data', '2016_fake_data.csv', '2016_real_data.csv')
    df = pd.read_csv(os.path.join('E:\\data', 'output_2016.csv'))
    #X = df.iloc[:, :-1].values
    #y = df.iloc[:, -1].values
    y = df['label'].values
    X = df.drop('label', axis=1).values

    columns_to_scale = [0 , 1, 15, 16]
    scaler = StandardScaler()
    column_transformer = ColumnTransformer(
        [('scaler', scaler, columns_to_scale)],
        remainder='passthrough'
    )
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=rand_seed)
    X_train = column_transformer.fit_transform(X_train)
    X_test = column_transformer.transform(X_test)

    X_train_tensor = torch.FloatTensor(X_train).to(device)
    X_test_tensor = torch.FloatTensor(X_test).to(device)
    y_train_tensor = torch.LongTensor(y_train).to(device)
    y_test_tensor = torch.LongTensor(y_test).to(device)

    train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
    test_dataset = TensorDataset(X_test_tensor, y_test_tensor)
    train_loader = DataLoader(dataset=train_dataset, batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(dataset=test_dataset, batch_size=batch_size, shuffle=False)

    model_mlp = MLP(X_train.shape[1]).to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model_mlp.parameters(), lr=lr_base)

    for epoch in range(epochs):
        print(f'Epoch {epoch+1}/{epochs}')
        train(model_mlp, train_loader, criterion, optimizer)
        test(model_mlp, test_loader)

    ouutput_model_dir = 'E:\\data'
    torch.save(model_mlp.state_dict(), os.path.join(ouutput_model_dir, 'mlp_model.pth'))
    # loaded_model = MLP(input_size, hidden_size, output_size)
    # loaded_model.load_state_dict(torch.load('mlp_model.pth'))
    
    
