import json

import pytz
import requests
from core_data_modules.logging import Logger

log = Logger(__name__)

_BASE_URL = "https://graph.facebook.com/v8.0"
_MAX_RESULTS_PER_PAGE = 100  # For paged requests, the maximum number of records to request in each page


class FacebookClient(object):
    def __init__(self, access_token):
        """
        :param access_token: Facebook access token.
        :type access_token: str
        """
        self._access_token = access_token

    @staticmethod
    def _date_to_facebook_time(date):
        """
        Converts a datetime into a format compatible with Facebook's API.

        Facebook only accepts dates as ISO 8601 strings in Zulu-time (UTC with a 'Z' indicator of timezone).

        :param date: Date to convert to Facebook time.
        :type date: datetime.datetime
        :return: `date` in a format compatible with Facebook's APIs.
        :rtype: str
        """
        return date.astimezone(pytz.utc).isoformat().replace("+00:00", "Z")

    def _make_get_request(self, endpoint, params=None):
        if params is None:
            params = {}
        params = params.copy()
        params["access_token"] = self._access_token

        url = f"{_BASE_URL}{endpoint}"
        response = requests.get(url, params)

        return response.json()

    def _make_paged_get_request(self, endpoint, params=None):
        if params is None:
            params = {}
        params = params.copy()
        params["access_token"] = self._access_token

        url = f"{_BASE_URL}{endpoint}"
        response = requests.get(url, params)
        if "data" not in response.json():
            log.error(f"Response from Facebook did not contain a 'data' field. "
                      f"The returned data is probably an error message: {response.json()}")
            exit(1)

        result = response.json()["data"]
        next_url = response.json().get("paging", {}).get("next")
        while next_url is not None:
            response = requests.get(next_url)
            if "data" not in response.json():
                log.error(f"Response from Facebook did not contain a 'data' field. "
                          f"The returned data is probably an error message: {response.json()}")
                exit(1)

            result.extend(response.json()["data"])
            next_url = response.json()["paging"].get("next")
        return result

    def get_post(self, post_id, fields=["created_time", "message", "id"]):
        """
        Gets the post with the given id.

        :param post_id: Id of post to download.
        :type post_id: str
        :param fields: Fields to include in the returned dict. `id` will always be included, even if not specified.
                       For available fields, see https://developers.facebook.com/docs/graph-api/reference/page-post.
        :type fields: iterable of str
        :return: Post with id `post_id`, as a dict containing the keys in `fields`.
        :rtype: dict
        """
        log.info(f"Fetching post '{post_id}'...")
        return self._make_get_request(
            f"/{post_id}",
            {
                "fields": ",".join(fields)
            }
        )

    def get_posts_published_by_page(self, page_id, fields=["attachments", "created_time", "message"],
                                    created_after=None, created_before=None):
        """
        Gets posts published by the given page.

        :param page_id: Id of the page to download all the posts from.
        :type page_id: str
        :param fields: Fields to include in the returned dict. `id` will always be included, even if not specified.
                       For available fields, see https://developers.facebook.com/docs/graph-api/reference/page-post.
        :type fields: iterable of str
        :param created_after: Start of the date-range to download posts from, by post created_on time, or None.
                              If None, posts will be downloaded from the beginning of time.
        :type created_after: datetime.datetime | None
        :param created_before: End of the date-range to download posts from, by post created_on time, or None.
                               If None, posts will be downloaded until the end of time.
        :type created_before: datetime.datetime | None
        :return: Posts published by page with id `page_id`, as a list of dicts containing the keys in `fields`.
        :rtype: list of dict
        """
        log_str = f"Fetching all posts published by page '{page_id}'"
        if created_after is not None:
            log_str += f", created after {created_after.isoformat()}"
        if created_before is not None:
            log_str += f", created after {created_before.isoformat()}"
        log.debug(f"{log_str}...")

        params = {
            "fields": ",".join(fields),
            "limit": _MAX_RESULTS_PER_PAGE
        }
        if created_after is not None:
            params["since"] = self._date_to_facebook_time(created_after)
        if created_before is not None:
            params["until"] = self._date_to_facebook_time(created_before)

        posts = self._make_paged_get_request(
            f"/{page_id}/published_posts",
            params
        )
        log.info(f"Fetched {len(posts)} posts")

        return posts

    def get_all_comments_on_post(self, post_id, fields=["parent", "attachments", "created_time", "message"],
                                 raw_export_log_file=None):
        """
        Gets all the comments on a post that are visible to this user, including comments which are replies to
        other comments.

        :param post_id: Post to download the comments from.
        :type post_id: str
        :param fields: Fields to include in the returned dict. `id` will always be included, even if not specified.
                       For available fields, see https://developers.facebook.com/docs/graph-api/reference/comment.
        :type fields: iterable of str
        :param raw_export_log_file: File to write the raw data downloaded during this function call to as json.
        :type raw_export_log_file: file-like | None
        :return: Comments on the post with id `post_id`, as a list of dicts containing the keys in `fields`.
        :rtype: list of dict
        """
        log.info(f"Fetching all comments on post '{post_id}'...")
        comments = self._make_paged_get_request(
            f"/{post_id}/comments",
            {
                "fields": ",".join(fields),
                "limit": _MAX_RESULTS_PER_PAGE,
                "filter": "stream"
            }
        )
        log.info(f"Fetched {len(comments)} comments")

        if raw_export_log_file is not None:
            log.info(f"Logging {len(comments)} fetched comments...")
            json.dump(comments, raw_export_log_file)
            raw_export_log_file.write("\n")
            log.info(f"Logged fetched comments")
        else:
            log.debug("Not logging the raw export (argument 'raw_export_log_file' was None)")

        return comments

    def get_raw_metrics_for_post(self, post_id, metrics):
        """
        Gets the raw metrics on a post in the full format returned by Facebook.

        For an easier-to-use dict of metric to value, see `FacebookClient.get_metrics_for_post`.

        :param post_id: Id of post to get metrics for.
        :type post_id: str
        :param metrics: Metrics to request from Facebook. For the list of available metrics,
                        see https://developers.facebook.com/docs/graph-api/reference/insights
        :type metrics: iterable of str
        :return: Requested metrics for this post, as returned by the Facebook API.
        :rtype: list of dict
        """
        return self._make_get_request(
            f"/{post_id}/insights?metric={','.join(metrics)}"
        )["data"]

    def get_metrics_for_post(self, post_id, metrics):
        """
        Gets the metrics on a post as a simple dict of metric -> value

        For the full data returned from Facebook, see `FacebookClient.get_raw_metrics_for_post`.

        :param post_id: Id of post to get metrics for.
        :type post_id: str
        :param metrics: Metrics to request from Facebook. For the list of available metrics,
                        see https://developers.facebook.com/docs/graph-api/reference/insights
        :type metrics: iterable of str
        :return: Requested metrics for this post, in the format metric -> value.
        :rtype: dict of str -> any
        """
        raw_metrics = self.get_raw_metrics_for_post(post_id, metrics)

        cleaned_metrics = dict()
        for m in raw_metrics:
            assert len(m["values"]) == 1, f"Metric {m['name']} has {len(m['values'])} values, but " \
                                          f"FacebookClient.get_metrics_for_post only expects one. " \
                                          f"Use `FacebookClient.get_raw_metrics_for_post` instead or report" \
                                          f"this to the developers of FacebookClient"
            cleaned_metrics[m["name"]] = m["values"][0]["value"]

        return cleaned_metrics
