import pandas as pd
import numpy as np
from sklearn.metrics import (
    mean_absolute_error, mean_squared_error, r2_score, explained_variance_score
)

class ModelEvaluator:
    def __init__(self, dataframe, observed_col, predicted_col):
        """
        Initialize the evaluator with the DataFrame and column names.

        Parameters:
        dataframe (pd.DataFrame): DataFrame containing the data.
        observed_col (str): Column name for observed values.
        predicted_col (str): Column name for predicted values.
        """
        self.dataframe = dataframe
        self.observed_col = observed_col
        self.predicted_col = predicted_col

    def calculate_metrics(self):
        """
        Calculate evaluation metrics and return as a dictionary.

        Returns:
        dict: Dictionary containing evaluation metrics.
        """
        self.dataframe = self.dataframe.dropna(subset=[self.observed_col, self.predicted_col])
        y_true = self.dataframe[self.observed_col]
        y_pred = self.dataframe[self.predicted_col]

        # Willmott's Index of Agreement (WI)
        mean_obs = y_true.mean()
        wi_numerator = ((y_true - y_pred).abs()).sum()
        wi_denominator = ((y_true - mean_obs).abs() + (y_pred - mean_obs).abs()).sum()
        wi = 1 - (wi_numerator / wi_denominator)

        # Nash–Sutcliffe Efficiency (NS)
        ns_numerator = ((y_true - y_pred) ** 2).sum()
        ns_denominator = ((y_true - mean_obs) ** 2).sum()
        ns = 1 - (ns_numerator / ns_denominator)

        # Mean Absolute Deviation (MAD)
        mad = (y_true - y_pred).abs().mean()

        metrics = {
            'Mean Absolute Error (MAE)': mean_absolute_error(y_true, y_pred),
            'Mean Squared Error (MSE)': mean_squared_error(y_true, y_pred),
            'Root Mean Squared Error (RMSE)': np.sqrt(mean_squared_error(y_true, y_pred)),
            'R-squared (R2)': r2_score(y_true, y_pred),
            'Explained Variance Score': explained_variance_score(y_true, y_pred),
            "Willmott's Index of Agreement (WI)": wi,
            'Nash–Sutcliffe Efficiency (NS)': ns,
            'Mean Absolute Deviation (MAD)': mad
        }
        return metrics

    def display_metrics(self):
        """
        Display evaluation metrics in a readable format.
        """
        metrics = self.calculate_metrics()
        print("Model Performance Metrics:")
        for metric, value in metrics.items():
            print(f"{metric}: {value:.4f}")
