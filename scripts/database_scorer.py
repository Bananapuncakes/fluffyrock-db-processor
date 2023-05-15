class DatabaseScorer:
    def __init__(self):
        self.BASE_POS_ONE = 0.5  # +
        self.BASE_POS_TWO = 1.25  # ++
        self.BASE_POS_THREE = 1.75  # +++
        self.BASE_POS_FOUR = 2.5  # ++++
        self.BASE_POS_FIVE = 3.0  # +++++
        self.BASE_NEG_ONE = -0.25  # -
        self.BASE_NEG_TWO = -1.0  # --
        self.BASE_NEG_THREE = -1.75  # ---
        self.BASE_NEG_FOUR = -3.0  # ----
        self.BASE_NEG_FIVE = -5.0  # -----
        self.BASE_NOT_APP = 0.0

    class TagScore:
        def __init__(self, tag_name, score):
            TAG_POS = self.BASE_POS_ONE  # Positive Tag
            TAG_HI_POS = self.BASE_POS_TWO  # High Positive Tag
            TAG_VHI_POS = self.BASE_POS_THREE  # Very Positive Tag
            TAG_COM = self.BASE_NEG_TWO  # Extremely Common Tag
            TAG_NEG = self.BASE_NOT_APP  # Negative Tag
            TAG_HI_NEG = self.BASE_NEG_TWO  # High Negative Tag
            TAG_VHI_NEG = self.BASE_NEG_THREE  # Very Negative Tag
            TAG_XHI_NEG = self.BASE_NEG_FOUR  # Extremely Negative Tag

        def average_positive_score(self, tag_name):
            pass

        def average_negative_score(self, tag_name):
            pass

    class ArtistScore:
        def __init__(self, artist_name, score):
            ARST_LOW_POS = self.BASE_POS_ONE  # Low Positive Score
            ARST_MED_POS = self.BASE_POS_TWO  # Medium Positive Score
            ARST_HI_POS = self.BASE_POS_THREE  # High Positive Score
            ARST_LOW_FAV = self.BASE_POS_ONE  # Low Favourite Count
            ARST_MED_FAV = self.BASE_POS_TWO  # Medium Favourite Count
            ARST_HI_FAV = self.BASE_POS_THREE  # High Favourite Count
            ARST_SLW_NEG = self.BASE_NOT_APP  # Slightly Low Negative Score
            ARST_LOW_NEG = self.BASE_NEG_ONE  # Low Negative Score
            ARST_MED_NEG = self.BASE_NEG_TWO  # Medium Negative Score
            ARST_HI_NEG = self.BASE_NEG_THREE  # High Negative Score
            ARST_AVE_ORG = self.BASE_NOT_APP  # Average Originality
            ARST_MED_ORG = self.BASE_POS_ONE  # Medium Originality
            ARST_HI_ORG = self.BASE_POS_TWO  # High Originality
            ARST_VHI_ORG = self.BASE_POS_FOUR  # Very High Originality
            ARST_LOW_ORG = self.BASE_NOT_APP  # Low Originality
            ARST_VLOW_ORG = self.BASE_NEG_ONE  # Very Low Originality
            ARST_POS_OUT = self.BASE_POS_TWO  # Positive Outlier Score
            ARST_VPOS_OUT = self.BASE_POS_FOUR  # Very Positive Outlier Score
            ARST_POS_POUT = self.BASE_POS_TWO  # Positive Outlier Low Post Number
            ARST_VPOS_POUT = self.BASE_POS_FOUR  # Very Positive Outlier Low Post Number
            ARST_LOW_CTRV = self.BASE_NOT_APP  # Low Tag Controversiality
            ARST_MED_CTRV = self.BASE_NEG_ONE  # Medium Tag Controversiality
            ARST_HI_CTRV = self.BASE_NEG_TWO  # High Tag Controversiality
            ARST_VHI_CTRV = self.BASE_NEG_THREE  # Very High Controversiality

        def average_positive_score(self, artist_name):
            pass

        def average_negative_score(self, artist_name):
            pass

        def average_originality_score(self, artist_name):
            pass

        def average_outlier_score(self, artist_name):
            pass

        def low_post_count(self, artist_name):
            pass

        def average_tag_controversy_score(self, artist_name):
            pass

    class PostScore:
        def __init__(self, post_id, score):
            POST_LOW_ARST = self.BASE_POS_ONE  # Low Positive Artist Score
            POST_MED_ARST = self.BASE_POS_TWO  # Medium Positive Artist Score
            POST_HI_ARST = self.BASE_POS_THREE  # High Positive Artist Score
            POST_VHI_ARST = self.BASE_POS_FOUR  # Very High Positive Artist Score
            POST_LOW_POS = self.BASE_POS_ONE  # Low Positive Score
            POST_MED_POS = self.BASE_POS_TWO  # Medium Positive Score
            POST_HI_POS = self.BASE_POS_THREE  # High Positive Score
            POST_VHI_POS = self.BASE_POS_FOUR  # Very High Positive Score
            POST_MED_ARST_OUT = self.BASE_POS_TWO  # Medium Artist Average Score Outlier
            POST_HI_ARST_OUT = self.BASE_POS_FOUR  # High Artist Average Score Outlier
            POST_VHI_ARST_OUT = (
                self.BASE_POS_FIVE
            )  # Very High Artist Average Score Outlier
            POST_LOW_ARST_OUT = (
                self.BASE_NEG_ONE
            )  # Low Negative Artist Average Score Outlier
            POST_VLOW_ARST_OUT = (
                self.BASE_NEG_TWO
            )  # Very Low Negative Artist Score Outlier
            POST_LOW_NEG = self.BASE_NOT_APP  # Low Negative Score
            POST_MED_NEG = self.BASE_NEG_ONE  # Medium Negative Score
            POST_HI_NEG = self.BASE_NEG_TWO  # High Negative Score
            POST_VHI_NEG = self.BASE_NEG_FOUR  # Very High Negative Score
            POST_LOW_CRS = self.BASE_NOT_APP  # Low Crossroads Score
            POST_MED_CRS = self.BASE_POS_ONE  # Medium Crossroads Score
            POST_HI_CRS = self.BASE_POS_TWO  # High Crossroads Score
            POST_VHI_CRS = self.BASE_NEG_ONE  # Very High Crossroads Score
            POST_LOW_TAG_CTRV = self.BASE_NOT_APP  # Low Controversial Tags
            POST_MED_TAG_CTRV = self.BASE_NEG_ONE  # Medium Controversial Tags
            POST_HI_TAG_CTRV = self.BASE_NEG_ONE  # High Controversial Tags
            POST_VHI_TAG_CTRV = self.BASE_NEG_ONE  # Very High Controversial Tags
            POST_DEF_BLK_TAG = self.BASE_NEG_ONE  # Default Blacklisted Tag
            POST_LOW_DEF_BLK_TAG = (
                self.BASE_NEG_TWO
            )  # Default Blacklisted Tag Low Score
            POST_VLOW_DEF_BLK_TAG = (
                self.BASE_NEG_FOUR
            )  # Default Blacklisted Tag Very Low Score
            POST_VREC_IMG = self.BASE_POS_TWO  # Very Recent Image
            POST_REC_IMG = self.BASE_POS_ONE  # Recent Image
            POST_OLD_IMG = self.BASE_NOT_APP  # Older Image

        def artist_score(self, artist_name):
            pass

        def positive_score(self, post_id):
            pass

        def outlier_score(self, post_id):
            pass

        def negative_score(self, post_id):
            pass

        def crossroads_score(self, post_id):
            pass

        def tag_controrversy_score(self, post_id):
            pass

        def default_blacklist_score(self, post_id):
            pass

        def image_age_score(self, post_id):
            pass
