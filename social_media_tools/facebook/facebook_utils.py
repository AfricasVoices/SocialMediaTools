from core_data_modules.data_models import validators
from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.util import TimeUtils
from dateutil.parser import isoparse

log = Logger(__name__)


def clean_post_type(post):
    """
    Cleans Facebook post type

    :param post: Facebook post in the format returned by Facebook's API.
    :type post: dict
    :return: "photo" | "video" | None
    :rtype: str | None
    """
    post_type = None
    for attachment in post["attachments"]["data"]:
        assert attachment["type"] in {"video_inline", "video_direct_response", "photo"}, post

        if attachment["type"] in {"video_inline", "video_direct_response"}:
            assert post_type in {"video", None}, post
            post_type = "video"
        elif attachment["type"] == "photo":
            assert post_type in {"photo", None}, post
            post_type = "photo"

    return post_type


def convert_facebook_comments_to_traced_data(user, dataset_name, raw_comments, facebook_uuid_table):
    log.info(f"Converting {len(raw_comments)} Facebook comments to TracedData...")

    facebook_uuids = {comment["from"]["id"] for comment in raw_comments}
    facebook_to_uuid_lut = facebook_uuid_table.data_to_uuid_batch(facebook_uuids)

    traced_comments = []
    # Use a placeholder avf facebook id for now, to make the individuals file work until we know if we'll be able
    # to see Facebook user ids or not.
    for comment in raw_comments:
        comment["created_time"] = isoparse(comment["created_time"]).isoformat()
        validators.validate_utc_iso_string(comment["created_time"])

        comment_dict = {
            "avf_facebook_id": facebook_to_uuid_lut[comment["from"]["id"]]
        }
        for k, v in comment.items():
            comment_dict[f"{dataset_name}.{k}"] = v

        traced_comments.append(
            TracedData(comment_dict,
                       Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())))

    log.info(f"Converted {len(traced_comments)} Facebook comments to TracedData")

    return traced_comments
