import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt


from tensorflow.keras.models import Model
from tensorflow.keras.layers import MultiHeadAttention, LayerNormalization, Dropout,Dense, Input, Masking, GlobalAveragePooling1D, Embedding, Lambda, LSTM, RepeatVector, TimeDistributed
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers.schedules import ExponentialDecay
from tensorflow.keras.regularizers import l2
from tensorflow.keras.layers import LeakyReLU
from tensorflow.keras import backend as K
from tensorflow.keras.losses import Huber


full_feature_list = sorted(['week_day', 'Open', 'High', 'Low', 'Close', 'Volume', 'SMA_30', 'SMA_40', 'EMA_10', 'EMA_30', 'MACD_12_26_9', 'MACDh_12_26_9', 'MACDs_12_26_9', 'RSI_10', 'BBL_5_2.0', 'BBM_5_2.0', 'BBU_5_2.0', 'BBB_5_2.0', 'BBP_5_2.0', 'avgTradingVolume', 'ADX_14', 'DMP_14', 'DMN_14', 'ISA_9', 'ISB_26', 'ITS_9', 'IKS_26', 'STOCHk_14_3_3', 'STOCHd_14_3_3', 'PSAR_combined', '52_week_high', '52_week_low', 'NASDAQ_Close', 'NASDAQ_EMA_10', 'NASDAQ_EMA_30', 'SP500_Close', 'SP500_EMA_10', 'SP500_EMA_30'])

features_to_keep = ['week_day', 'Open', 'High', 'Low', 'Close', 'Volume','SMA_30', 'SMA_40', 'EMA_10', 'EMA_30', 'MACDh_12_26_9', 'RSI_10', 'BBP_5_2.0', 'avgTradingVolume', 'STOCHk_14_3_3', '52_week_high', '52_week_low', 'NASDAQ_Close', 'STOCHd_14_3_3', 'ADX_14', 'DMP_14', 'DMN_14', 'ISA_9', 'ISB_26', 'ITS_9', 'IKS_26', 'PSAR_combined']

feature_list = sorted([feature for feature in full_feature_list if feature in features_to_keep])


# Define the Transformer's parameters
d_model = 512  # Embedding dimension

num_heads = 8  # Number of attention heads
dff = 1024  # Dimension of the feed-forward network
num_layers = 6  # Number of encoder and decoder layers
dropout_rate = 0.1  # Dropout rate

# Constant learning rate
constant_learning_rate = 1e-6

# [32, 20, x]
seq_length = 30  # Length of your input sequences
feature_size = len(feature_list)  # Number of features in your dataset



def custom_loss(y_true, y_pred):
    next_day_open_price = tf.expand_dims(y_true[:, 1], axis=-1)

    return tf.keras.losses.mean_squared_error(next_day_open_price, y_pred)


def directional_accuracy(y_true, y_pred):
    last_day_opening_price = tf.expand_dims(y_true[:, 0], axis=-1)
    next_day_open_price = tf.expand_dims(y_true[:, 1], axis=-1)

    sign_true = K.sign(next_day_open_price - last_day_opening_price)
    sign_pred = K.sign(y_pred - last_day_opening_price)

    return K.mean(K.equal(sign_true, sign_pred), axis=-1)

def positional_encoding(seq_length, num_features):
    """
    Create positional encodings for the input.

    Parameters:
    seq_length (int): The length of the sequence.
    num_features (int): The number of features being encoded.

    Returns:
    np.ndarray: A seq_length x num_features array of positional encodings.
    """

    # Initialize the positional encoding matrix
    position = np.arange(seq_length)[:, np.newaxis]
    div_term = np.exp(np.arange(0, num_features, 2) * -(np.log(10000.0) / num_features))

    # Compute the positional encodings
    pe = np.zeros((seq_length, num_features))
    pe[:, 0::2] = np.sin(position * div_term)
    pe[:, 1::2] = np.cos(position * div_term)

    return pe

def transformer_encoder_layer(d_model, num_heads, dff, dropout_rate, name):
    inputs = Input(shape=(None, d_model))
    attention = MultiHeadAttention(num_heads=num_heads, key_dim=d_model)(inputs, inputs)
    attention = Dropout(dropout_rate)(attention)
    attention = LayerNormalization(epsilon=1e-6)(inputs + attention)

    outputs = Dense(dff, activation='relu')(attention)
    outputs = Dense(d_model)(outputs)
    outputs = Dropout(dropout_rate)(outputs)
    outputs = LayerNormalization(epsilon=1e-6)(attention + outputs)

    return Model(inputs=inputs, outputs=outputs, name=name)

def transformer_decoder_layer(d_model, num_heads, dff, dropout_rate, name):
    inputs = Input(shape=(None, d_model))
    enc_outputs = Input(shape=(None, d_model))

    # Create a look-ahead mask for masked self-attention
    seq_length = tf.shape(inputs)[1]
    look_ahead_mask = 1 - tf.linalg.band_part(tf.ones((seq_length, seq_length)), -1, 0)

    attention1 = MultiHeadAttention(num_heads=num_heads, key_dim=d_model)(inputs, inputs, attention_mask=look_ahead_mask)
    attention1 = Dropout(dropout_rate)(attention1)
    attention1 = LayerNormalization(epsilon=1e-6)(attention1 + inputs)

    attention2 = MultiHeadAttention(num_heads=num_heads, key_dim=d_model)(attention1, enc_outputs)
    attention2 = Dropout(dropout_rate)(attention2)
    attention2 = LayerNormalization(epsilon=1e-6)(attention2 + attention1)

    outputs = Dense(dff, activation='relu')(attention2)
    outputs = Dense(d_model)(outputs)
    outputs = Dropout(dropout_rate)(outputs)
    outputs = LayerNormalization(epsilon=1e-6)(outputs + attention2)

    return Model(inputs=[inputs, enc_outputs], outputs=outputs, name=name)

def transformer_model(seq_length, feature_size, d_model, num_heads, dff, num_layers, dropout_rate):
    inputs = Input(shape=(seq_length, feature_size))

    # Ensure that the encoder model is not trainable
    # encoder_model.trainable = False

    # Extracting the open prices and applying positional encoding
    open_prices = inputs[:, :, 21:22]  # Assuming 0-based indexing, the 11th feature is at index 10
    open_prices_pos_encoding = positional_encoding(seq_length, d_model)
    open_prices += open_prices_pos_encoding

    # feature_embeddings = encoder_model(inputs)
    feature_embeddings = Dense(d_model, activation='leaky_relu')(inputs)
    # feature_embeddings = LayerNormalization(epsilon=1e-6)(feature_embeddings)
    pos_encoding = positional_encoding(seq_length, d_model)
    feature_embeddings += pos_encoding

    x = feature_embeddings
    for i in range(num_layers):
        x = transformer_encoder_layer(d_model, num_heads, dff, dropout_rate, f"encoder_layer_{i+1}")(x)

    encoder_output = x

    # Preparing the decoder inputs by expanding the dimensions of the open prices to match the encoder output
    decoder_output = open_prices
    # Passing the combined inputs through the decoder layers
    for i in range(num_layers):
        decoder_output = transformer_decoder_layer(d_model, num_heads, dff, dropout_rate, f"decoder_layer_{i+1}")([decoder_output, encoder_output])

    decoder_output = Lambda(lambda x: x[:, -1, :])(decoder_output)

    outputs = Dense(1, activation='linear')(decoder_output)

    model = Model(inputs=inputs, outputs=outputs)
    # model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=constant_learning_rate),
    #             loss=custom_loss,
    #             metrics=[directional_accuracy])
    # metrics=[directional_accuracy, tf.keras.metrics.MeanSquaredError(name="MSE"), tf.keras.metrics.MeanAbsoluteError(name="MAE"), tf.keras.metrics.MeanAbsolutePercentageError(name="MAPE")])
    return model