# -------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------
import os
import shutil
import unittest

import numpy as np
from sklearn import svm
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split, KFold
from sklearn.preprocessing import MinMaxScaler

from systemds.scuro.aligner.dr_search import DRSearch
from systemds.scuro.aligner.task import Task
from systemds.scuro.modality.audio_modality import AudioModality
from systemds.scuro.modality.text_modality import TextModality
from systemds.scuro.modality.video_modality import VideoModality
from systemds.scuro.models.model import Model
from systemds.scuro.representations.average import Average
from systemds.scuro.representations.bert import Bert
from systemds.scuro.representations.concatenation import Concatenation
from systemds.scuro.representations.lstm import LSTM
from systemds.scuro.representations.max import RowMax
from systemds.scuro.representations.mel_spectrogram import MelSpectrogram
from systemds.scuro.representations.multiplication import Multiplication
from systemds.scuro.representations.resnet import ResNet
from systemds.scuro.representations.sum import Sum
from tests.scuro.data_generator import TestDataGenerator

import warnings

warnings.filterwarnings("always")


class TestSVM(Model):
    def __init__(self):
        super().__init__("Test")

    def fit(self, X, y, X_test, y_test):
        self.clf = svm.SVC(C=1, gamma="scale", kernel="rbf", verbose=False)
        self.clf = self.clf.fit(X, np.array(y))
        y_pred = self.clf.predict(X)

        return classification_report(
            y, y_pred, output_dict=True, digits=3, zero_division=1
        )["accuracy"]

    def test(self, test_X: np.ndarray, test_y: np.ndarray):
        y_pred = self.clf.predict(np.array(test_X))  # noqa

        return classification_report(
            np.array(test_y), y_pred, output_dict=True, digits=3, zero_division=1
        )["accuracy"]


def scale_data(data, train_indizes):
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaler.fit(data[train_indizes])
    return scaler.transform(data)


class TestDataLoaders(unittest.TestCase):
    train_indizes = None
    val_indizes = None
    test_file_path = None
    mods = None
    text = None
    audio = None
    video = None
    data_generator = None
    num_instances = 0
    indizes = []
    representations = None

    @classmethod
    def setUpClass(cls):
        cls.test_file_path = "test_data_dr_search"

        if os.path.isdir(cls.test_file_path):
            shutil.rmtree(cls.test_file_path)

        os.makedirs(f"{cls.test_file_path}/embeddings")

        cls.num_instances = 8
        cls.indizes = [str(i) for i in range(0, cls.num_instances)]
        cls.video = VideoModality(
            "", ResNet(f"{cls.test_file_path}/embeddings/resnet_embeddings.hdf5")
        )
        cls.audio = AudioModality(
            "",
            MelSpectrogram(
                output_file=f"{cls.test_file_path}/embeddings/mel_sp_embeddings.npy"
            ),
        )
        cls.text = TextModality(
            "",
            Bert(
                avg_layers=4,
                output_file=f"{cls.test_file_path}/embeddings/bert_embeddings.pkl",
            ),
        )
        cls.mods = [cls.video, cls.audio, cls.text]
        cls.data_generator = TestDataGenerator(cls.mods, cls.test_file_path)
        cls.data_generator.create_multimodal_data(cls.num_instances)
        cls.text.read_all(cls.indizes)
        cls.audio.read_all(cls.indizes)
        cls.video.read_all([i for i in range(0, cls.num_instances)])

        split = train_test_split(
            cls.indizes, cls.data_generator.labels, test_size=0.2, random_state=42
        )
        cls.train_indizes, cls.val_indizes = [int(i) for i in split[0]], [
            int(i) for i in split[1]
        ]

        for m in cls.mods:
            m.data = scale_data(m.data, [int(i) for i in cls.train_indizes])

        cls.representations = [
            Concatenation(),
            Average(),
            RowMax(),
            Multiplication(),
            Sum(),
            LSTM(width=256, depth=3),
        ]

    @classmethod
    def tearDownClass(cls):
        print("Cleaning up test data")
        shutil.rmtree(cls.test_file_path)

    def test_enumerate_all(self):
        task = Task(
            "TestTask",
            TestSVM(),
            self.data_generator.labels,
            self.train_indizes,
            self.val_indizes,
        )
        dr_search = DRSearch(self.mods, task, self.representations)
        best_representation, best_score, best_modalities = dr_search.fit_enumerate_all()

        for r in dr_search.scores.values():
            for scores in r.values():
                assert scores[1] <= best_score

    def test_enumerate_all_vs_random(self):
        task = Task(
            "TestTask",
            TestSVM(),
            self.data_generator.labels,
            self.train_indizes,
            self.val_indizes,
        )
        dr_search = DRSearch(self.mods, task, self.representations)
        best_representation_enum, best_score_enum, best_modalities_enum = (
            dr_search.fit_enumerate_all()
        )

        dr_search.reset_best_params()

        best_representation_rand, best_score_rand, best_modalities_rand = (
            dr_search.fit_random(seed=42)
        )

        assert best_score_rand <= best_score_enum


if __name__ == "__main__":
    unittest.main()
