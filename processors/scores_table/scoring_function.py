import math


class ScoringFunction:
    """
    This class is used to construct a parameterizable function that returns
    a score based on previews and full_views.
    """
    def __init__(self, max_coldness_score=40, min_previews_threshold=30, cold_threshold_steepness=0.35,
                 max_hotness_score=60, ctr_hotness_threshold=0.15, hot_threshold_steepness=13,
                 score_offset = -50):
        """

        :param max_coldness_score: max points possible for cold_start posts
        :param min_previews_threshold: soft threshold for when cold_start weighting declines (50% of max value)
        :param cold_threshold_steepness: steepness of sigmoid function
        :param max_hotness_score: max points possible for high click through rate posts
        :param ctr_hotness_threshold: soft threshold for when hotness weighting increases(50% of max value)
        :param hot_threshold_steepness: steepness of sigmoid function
        """
        self.max_coldness_score = max_coldness_score
        self.min_previews_threshold = min_previews_threshold
        self.cold_threshold_steepness = cold_threshold_steepness
        self.max_hotness_score = max_hotness_score
        self.ctr_hotness_threshold = ctr_hotness_threshold
        self.hot_threshold_steepness = hot_threshold_steepness
        self.score_offset = score_offset

    def score(self, previews, full_views):
        return self.hotness_score(previews, full_views) + self.coldness_score(previews) + self.score_offset

    def hotness_score(self, previews, full_views):
        if previews + full_views == 0:
            click_thru_rate = 0
        else:
            click_thru_rate = full_views / max(previews, full_views)
        # max fn guards against edge case of out of ordering of preview and view event delivery

        hotness_weight = 1.0 / (1.0 + math.exp(-self.hot_threshold_steepness * (click_thru_rate - self.ctr_hotness_threshold)))
        return hotness_weight * self.max_hotness_score

    def coldness_score(self, previews):
        coldness_weight = 1 - 1 / (1 + math.exp( -self.cold_threshold_steepness * (previews - self.min_previews_threshold)))
        return coldness_weight * self.max_coldness_score

    def get_config(self):
        """

        :return: dictionary of parameters for this function.
        """
        return {'max_coldness_score': self.max_coldness_score,
                'min_previews_threshold': self.min_previews_threshold,
                'cold_threshold_steepness': self.cold_threshold_steepness,
                 'max_hotness_score': self.max_hotness_score,
                'ctr_hotness_threshold': self.ctr_hotness_threshold,
                'hot_threshold_steepness': self.hot_threshold_steepness,
                 'score_offset': self.hot_threshold_steepness}
