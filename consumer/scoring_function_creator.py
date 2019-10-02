import math


class ScoringFunctionCreator:
    def __init__(self, max_coldness_score=50, min_previews_threshold=40, cold_threshold_steepness=0.35,
                 max_hotness_score=50, ctr_hotness_threshold=0.12, hot_threshold_steepness=17):
        self.max_coldness_score = max_coldness_score
        self.min_previews_threshold = min_previews_threshold
        self.cold_threshold_steepness = cold_threshold_steepness
        self.max_hotness_score = max_hotness_score
        self.ctr_hotness_threshold = ctr_hotness_threshold
        self.hot_threshold_steepness = hot_threshold_steepness

    def score(self, previews, full_views):
        return self.hotness_score(previews, full_views) + self.coldness_score(previews)

    def hotness_score(self, previews, full_views):
        if previews + full_views == 0:
            return 0
        # max fn guards against edge case of out of ordering of preview and view event delivery
        click_thru_rate = full_views / max(previews, full_views)
        hotness_weight = 1.0 / (1.0 + math.exp(-self.hot_threshold_steepness * (click_thru_rate - self.ctr_hotness_threshold)))
        return hotness_weight * self.max_hotness_score

    def coldness_score(self, previews):
        coldness_weight = 1 - 1 / (1 + math.exp( -self.cold_threshold_steepness * (previews - self.min_previews_threshold)))
        return coldness_weight * self.max_coldness_score