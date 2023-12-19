import tensorflow as tf
import json
import os
import sys

def _float_feature(value):
    """Returns a float_list from a float / double / int / uint."""
    if value is None:
        return tf.train.Feature(float_list=tf.train.FloatList(value=[-2.0]))
    else:
        return tf.train.Feature(float_list=tf.train.FloatList(value=[float(value)]))

def convert_example_to_tfrecord(example, example_index, filename):
    """Converts a single example into a tf.train.Example."""
    features = {}

    # 'X' is a list of dictionaries, where each dictionary represents a day's data
    for day_index, daily_features in enumerate(example['X']):
        # Sort the features by key to ensure consistent ordering
        sorted_features = sorted(daily_features.items())
        
        for index, (key, value) in enumerate(sorted_features):
            feature_key = f"{key}_{day_index}"
            features[feature_key] = _float_feature(value)

    features['label'] = _float_feature(example['Y'])

    return tf.train.Example(features=tf.train.Features(feature=features))

def convert_examples_to_tf_records(input_directory, split):
    output_file = os.path.join(input_directory, f'{split}_data.tfrecord')
    writer = tf.io.TFRecordWriter(output_file)

    for subfolder in os.listdir(input_directory):
        subfolder_path = os.path.join(input_directory, subfolder)
        if os.path.isdir(subfolder_path):
            for filename in os.listdir(subfolder_path):
                if filename.endswith('.json'):
                    file_path = os.path.join(subfolder_path, filename)
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        for idx, (example_key, example_data) in enumerate(data.items()):
                            tf_example = convert_example_to_tfrecord(example_data, idx, filename)
                            writer.write(tf_example.SerializeToString())

    writer.close()
    print(f"TFRecord file created at: {output_file}")

def count_records_in_tfrecord(tfrecord_file):
    count = 0
    for _ in tf.data.TFRecordDataset(tfrecord_file):
        count += 1
    return count

def main():
    split = 'validation'
    input_directory = f'G:/StockData/{split}_sequence_30_split_85'
    convert_examples_to_tf_records(input_directory, split)

    num_examples = count_records_in_tfrecord(f'{input_directory}/{split}_data.tfrecord')
    print(f"Number of examples in TFRecord file: {num_examples}")

if __name__ == "__main__":
    main()
