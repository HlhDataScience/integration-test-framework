from typing import Any

import openai
from evals import Eval
import tiktoken
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np


class IntegrationEvals(Eval):

    def __init__(self, sample_jsonl,model_name, **kwargs):

        super().__init__(**kwargs)
        self.samples_jsonl = sample_jsonl
        self.model_name = model_name
        self.client = openai.OpenAI()

    def run(self, recorder):

        samples = self.get_samples(self.samples_jsonl)
        self.eval_all_samples(recorder, samples)
        data_integration_scores = [e for e in recorder.get_scores("data_integration")]
        integration_index_score = [e for e in recorder.get_scores("integration_index")]

        return {
            "mean_data_integration" : float(np.mean(data_integration_scores)),
            "median_data_integration" : float(np.median(data_integration_scores)),
            "mean_integration_index" : float(np.mean(integration_index_score)),
            "median_integration_index": float(np.median(integration_index_score)),
        }

    def eval_sample(self, sample, rng):

        actual = sample["actual_response"]
        expected = sample["expected_response"]
        context = sample["context"]

        actual_response_embedding = self.client.embeddings.create(input=actual, model="text-embedding-3-small")
        expected_response_embedding = self.client.embeddings.create(input=expected, model="text-embedding-3-small")
        context_embedding = self.client.embeddings.create(input=context, model="text-embedding-3-small")

        data_integration = cosine_similarity(actual_response_embedding, expected_response_embedding)
        context_relevance = cosine_similarity(actual_response_embedding, context_embedding)
        response_relevance = cosine_similarity(actual_response_embedding, expected_response_embedding)

        perplexity_encoding = tiktoken.encoding_for_model(self.model_name)
        perplexity_encoded = perplexity_encoding.encode(actual)

        result = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role":"user","content":actual}],
            max_tokens=0, temperature=0,
            logprobs=False
        )
        logps = result.choices[0].logprobs.token_logprobs
        perplexity = float(np.exp(-np.mean(logps)))

        integration_index = np.mean([perplexity, context_relevance, response_relevance])

        return {
            "data_integration": data_integration,
            "context_relevance": context_relevance,
            "response_relevance": response_relevance,
            "perplexity": perplexity,
            "integration_index": integration_index,
        }