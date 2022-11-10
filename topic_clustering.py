# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

import numpy as np
import pandas as pd
from sklearn.metrics import silhouette_score
import tensorflow.compat.v2 as tf
import tensorflow_hub as hub
import tensorflow_text

# This flag disables GPU usage. Comment to use GPU with tensorflow.
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

CLUSTER_LABELS_FILE = "cluster_labels.txt"


class TopicClustering(object):
  """Handles the clustering of reviews into topics."""

  def __init__(self):
    # Reduce verbosity of tensorflow
    tf.get_logger().setLevel("ERROR")
    default_folder = os.path.dirname(os.path.realpath(__file__))
    self.cluster_labels_file_location = os.path.join(
        default_folder, CLUSTER_LABELS_FILE
    )

    self.model = hub.load(
        "https://tfhub.dev/google/universal-sentence-encoder-multilingual/3"
    )

    self.candidate_cluster_names = []

    if os.path.isfile(self.cluster_labels_file_location):
      with open(self.cluster_labels_file_location, "r") as labels_file:
        self.candidate_cluster_names = labels_file.read().splitlines()
        logging.info(
            "Found cluster labels file. %d labels loaded.",
            len(self.candidate_cluster_names),
        )

      labels_file.close()

  def recommend_topics(self, nouns):
    """Recommends a list of topics for a given set of nouns based on repetition.

    Args:
      nouns: a list with nouns for each review.

    Returns:
      The recommended list of topics.
    """
    nouns = [s.replace("translated by google", " ") for s in nouns]
    candidate_cluster_names = (
        pd.Series(" ".join(nouns).split()).value_counts()[0:150].index.to_list()
    )

    with open(self.cluster_labels_file_location, "w") as labels_file:
      for label in candidate_cluster_names:
        labels_file.write(label + "\n")

      labels_file.close()

    return candidate_cluster_names

  def determine_topics(self, reviews):
    """Determines the topic for a given set of reviews.

    Args:
      reviews: the full set of reviews to classify. This is modified to add
        a topic field with the calculated topic for each review.

    Returns:
      Nothing.
    """
    nouns = [
        self.extract_tokens(review["annotation"]["tokens"], "NOUN")
        for review in reviews
    ]

    if not self.candidate_cluster_names:
      self.candidate_cluster_names = self.recommend_topics(nouns)

    topics = self.modelling_pipeline(pd.DataFrame(nouns), [5, 10])

    topics = topics.to_list()
    for review in reviews:
      review["topic"] = topics.pop(0)
    return

  def extract_tokens(self, token_syntax, tag):
    """Extracts specified token type for API request.

    Args:
      token_syntax: API request return from Language API
      tag: type of token to return e.g. "NOUN" or "ADJ"

    Returns:
      string containing only words of specified syntax in API request
    """
    return " ".join(
        [
            s["lemma"].lower()
            for s in token_syntax
            if s["partOfSpeech"]["tag"] == tag
        ]
    )

  def modelling_pipeline(self, reviews, num_clusters_list, max_iterations=10):
    """Runs the clustering modelling pipeline with k-means.

    Args:
      reviews: pandas series of strings to assign to clusters
      num_clusters_list: a list of the number of clusters to attempt. The
        modelling pipeline will select the number with the best silhoutte
        coefficient
      max_iterations: the maximum number of iterations for k-means to perform

    Returns:
      numpy array containing the cluster names corresponding to reviews.
    """
    if not isinstance(num_clusters_list, list):
      raise ValueError("num_clusters_list is not a list")
    vectors = self.model(reviews)

    scores = [
        self.generate_silhouette_score(vectors, k, max_iterations)
        for k in num_clusters_list
    ]

    scores = dict(zip(num_clusters_list, scores))
    best_silhouette_score = max(scores, key=scores.get)
    logging.info(
        f"Optimal clusters is {best_silhouette_score} with silhouette score"
        f" {scores[best_silhouette_score]}"
    )

    cluster_indices, cluster_centers = self.generate_clusters(
        vectors, best_silhouette_score, max_iterations
    )

    index = self.return_most_similar_index(
        cluster_centers, self.model(self.candidate_cluster_names)
    )

    cluster_names = dict(
        zip(
            np.arange(len(cluster_centers)),
            [self.candidate_cluster_names[i] for i in list(index)],
        )
    )

    return pd.Series(cluster_indices).map(cluster_names)

  def generate_silhouette_score(
      self, vectors, num_clusters, max_iterations=10, seed=32
  ):
    """Generates the silhouette score of the clustering model.

    The Silhouette Coefficient is calculated using the mean intra-cluster
    distance (a) and the mean nearest-cluster distance (b) for each sample.
    The best value is 1 and the worst value is -1. Values near 0 indicate
    overlapping clusters. Negative values generally indicate that a sample has
    been assigned to the wrong cluster, as a different cluster is more similar.

    Args:
        vectors: Tensor containing the embeddings of the review
        num_clusters: the number of clusters to use
        max_iterations: the maximum number of iterations for k-means to perform
        seed: seed

    Returns:
        silhouette score as a float
    """
    cluster_indices, _ = self.generate_clusters(
        vectors, num_clusters, max_iterations=max_iterations, seed=seed
    )

    score = silhouette_score(vectors.numpy(), np.array(cluster_indices))

    logging.info(f"{num_clusters} clusters yields {score} silhouette score")
    return score

  def generate_clusters(
      self, vectors, num_clusters, max_iterations=10, seed=32
  ):
    """Generates clusters using vectors using K-means on cosine distance.

    Args:
      vectors: Tensor containing the embeddings of the reviews
      num_clusters: the number of clusters to use
      max_iterations: the maximum number of iterations for k-means to perform
      seed: seed

    Returns:
      df with named topics
    """
    kmeans = tf.compat.v1.estimator.experimental.KMeans(
        num_clusters=num_clusters,
        use_mini_batch=False,
        seed=seed,
        distance_metric=tf.compat.v1.estimator.experimental.KMeans.COSINE_DISTANCE,
    )

    def input_fn():
      return tf.compat.v1.train.limit_epochs(
          # first convert to numpy due to v1 & eager incompatability
          tf.convert_to_tensor(vectors.numpy(), dtype=tf.float32),
          num_epochs=1,
      )

    previous_centers = None
    score = 0

    for i in range(max_iterations):
      kmeans.train(input_fn)
      cluster_centers = kmeans.cluster_centers()
      if previous_centers is not None:
        previous_centers = cluster_centers
      new_score = kmeans.score(input_fn)  # sum of squared distances
      # break if score improves by less than (arbitrary) 10%
      logging.debug(
          "Iteration %d - Sum of squared distances: %.0f", i, new_score
      )
      if np.divide(score, new_score) > 1.1 or score == 0:
        score = new_score
      else:
        break

    return list(kmeans.predict_cluster_index(input_fn)), cluster_centers

  def return_most_similar_index(self, a, b, limit_cosine_similarity=0):
    """Returns the elements in b with the highest cosine similarity in a.

    limit_cosine_similarity sets a lower bound limit on the cosine similarity
    for an element to be returned (and returns -1 for these values).

    Args:
      a: Tensor of vectors
      b: Tensor of vectors
      limit_cosine_similarity: integer between 0 and 1
    """
    similarity = tf.reduce_sum(a[:, tf.newaxis] * b, axis=-1)

    similarity = tf.math.divide(
        similarity, tf.norm(a[:, tf.newaxis], axis=-1) * tf.norm(b, axis=-1)
    )

    indices = tf.math.argmax(similarity, axis=1).numpy()
    if limit_cosine_similarity > 0:
      max_cosine_similarity = tf.math.reduce_max(similarity, axis=1).numpy()
      indices[max_cosine_similarity < limit_cosine_similarity] = -1

    return indices
