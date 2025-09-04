#!/usr/bin/env python3

import os
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pymongo import MongoClient
from dotenv import load_dotenv

import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError

load_dotenv()

logger = logging.getLogger(__name__)


class MailchimpNotifier:
    def __init__(self,
                 mailchimp_api_key: str,
                 mongo_db: str = "business-broker-las-vegas-db",
                 mongo_collection: str = "bizbuysell-data") -> None:
        self.api_key = mailchimp_api_key
        self.list_id = os.getenv("MAILCHIMP_LIST_ID")
        self.template_id = int(os.getenv("MAILCHIMP_EMAIL_TEMPLATE_ID"))

        self.mongo_uri = os.getenv('MONGO_DB_URI')
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

        self.mailchimp_client = MailchimpMarketing.Client()
        self.__initialize_mailchimp_client()

    def __initialize_mailchimp_client(self):
        try:
            self.mailchimp_client.set_config({
                "api_key": self.api_key,
                "server": self.api_key.split('-')[-1]
            })
        except ApiClientError as error:
            logger.error(f"Mailchimp client initialization error: {error.text}")

    # -----------------------------
    # HTML Generation for Template
    # -----------------------------
    def _format_single_listing_html(self, listing: Dict) -> str:
        """Format a single listing as HTML."""
        title = listing.get('title', 'N/A')
        asking_price = listing.get('asking_price', 'N/A')
        broker_name = listing.get('broker_name', 'N/A')
        cash_flow = listing.get('cashflow', 'N/A')
        broker_phone = listing.get('broker_phone', 'N/A')
        description = listing.get('description', 'N/A')
        listing_url = listing.get('url') or listing.get('listing_url', '#')

        # Truncate description if too long
        if len(str(description)) > 300:
            description = str(description)[:300] + "..."

        return f"""
        <div style="margin-bottom: 30px; padding: 20px; border: 2px solid #e0e0e0; border-radius: 8px; background-color: #fafafa;">
            <div style="margin-bottom: 15px;">
                <p style="text-align: left; margin: 0 0 5px 0; font-weight: bold; color: #007cba; font-size: 16px;">TITLE</p>
                <p style="text-align: left; margin: 0 0 15px 0; font-size: 18px; font-weight: bold; color: #333;">{title}</p>
            </div>
            <div style="margin-bottom: 15px;">
                <p style="text-align: left; margin: 0 0 5px 0; font-weight: bold; color: #007cba; font-size: 16px;">ASKING PRICE</p>
                <p style="text-align: left; margin: 0 0 15px 0; font-size: 16px; color: #333; font-weight: bold;">{asking_price}</p>
            </div>
            <div style="margin-bottom: 15px;">
                <p style="text-align: left; margin: 0 0 5px 0; font-weight: bold; color: #007cba; font-size: 16px;">Sellers Discretionary Earnings (SDE):</p>
                <p style="text-align: left; margin: 0 0 15px 0; font-size: 16px; color: #333;">{cash_flow}</p>
            </div>
            <div style="margin-bottom: 15px;">
                <p style="text-align: left; margin: 0 0 5px 0; font-weight: bold; color: #007cba; font-size: 16px;">BROKER'S NAME</p>
                <p style="text-align: left; margin: 0 0 15px 0; font-size: 16px; color: #333;">{broker_name}</p>
            </div>
            <div style="margin-bottom: 15px;">
                <p style="text-align: left; margin: 0 0 5px 0; font-weight: bold; color: #007cba; font-size: 16px;">BROKER'S PHONE</p>
                <p style="text-align: left; margin: 0 0 15px 0; font-size: 16px; color: #333;">{broker_phone}</p>
            </div>
            <div style="margin-bottom: 15px;">
                <p style="text-align: left; margin: 0 0 5px 0; font-weight: bold; color: #007cba; font-size: 16px;">DESCRIPTION</p>
                <p style="text-align: left; margin: 0 0 15px 0; font-size: 14px; color: #333; line-height: 1.4;">{description}</p>
            </div>
            <div style="margin-bottom: 0;">
                <p style="text-align: left; margin: 0; font-size: 16px;">
                    <a href="{listing_url}" target="_blank" style="color: #007cba; text-decoration: none; font-weight: bold;">View Full Details →</a>
                </p>
            </div>
        </div>
        """

    def _generate_listings_html(self, listings: List[Dict]) -> str:
        """Generate HTML for all listings."""
        if not listings:
            return """
            <div style="text-align: center; padding: 20px; background-color: #f9f9f9; border-radius: 5px;">
                <p style="color: #666; font-size: 16px; margin: 0;">No new listings match your criteria at this time.</p>
                <p style="color: #666; font-size: 14px; margin: 10px 0 0 0;">We'll notify you when new opportunities become available!</p>
            </div>
            """

        html_parts = []
        for i, listing in enumerate(listings, 1):
            html_parts.append(f"""
            <div style="margin-bottom: 20px;">
                <h2 style="color: #007cba; font-size: 20px; margin: 0 0 10px 0;">Business Opportunity #{i}</h2>
                {self._format_single_listing_html(listing)}
            </div>
            """)

        return "".join(html_parts)

    # -----------------------------
    # Data fetchers
    # -----------------------------
    def fetch_subscribers(self):
        """Fetch all subscribers from the Mailchimp list."""
        try:
            return self.mailchimp_client.lists.get_list_members_info(self.list_id, count=1000)
        except ApiClientError as error:
            logger.error(f"Error fetching subscribers: {error.text}")
            return {"members": []}

    # -----------------------------
    # Matching logic
    # -----------------------------
    @staticmethod
    def _normalize_price(value) -> Optional[float]:
        """Normalize price value to float."""
        if value is None:
            return None
        try:
            s = str(value).replace(',', '').replace('$', '').strip()
            return float(s)
        except Exception:
            return None

    @staticmethod
    def _normalize_list_field(field_value: str) -> List[str]:
        """Normalize comma-separated string to list."""
        if not field_value:
            return []
        return [item.strip() for item in str(field_value).replace('\xa0', '').split(',')
                if item and item.strip()]

    def match_subscribers_to_listings(self, subs: List[Dict], listings: List[Dict]) -> Dict[str, List[Dict]]:
        """Match subscribers to listings based on their preferences."""
        matches = {}

        for sub in subs:
            email = sub.get('email_address')
            if not email:
                continue

            merge_fields = sub.get('merge_fields', {})
            if not merge_fields:
                continue

            # Get subscriber preferences
            min_price = merge_fields.get('DPR_MIN')
            max_price = merge_fields.get('DPR_MAX')
            subscriber_industries = set(i.lower() for i in self._normalize_list_field(merge_fields.get('INDUSTRIES')))
            subscriber_states = set(i.lower() for i in self._normalize_list_field(merge_fields.get('STATES')))
            subscriber_cities = set(i.lower() for i in self._normalize_list_field(merge_fields.get('CITIES')))

            # Check each listing
            matched_listings = []
            for listing in listings:
                if self._listing_matches_subscriber(listing, min_price, max_price,
                                                    subscriber_industries, subscriber_states, subscriber_cities):
                    matched_listings.append(listing)

            if matched_listings:
                matches[email] = matched_listings

        return matches

    def _listing_matches_subscriber(self, listing: Dict, min_price: Optional[float], max_price: Optional[float],
                                    subscriber_industries: set, subscriber_states: set, subscriber_cities: set) -> bool:
        """Check if a listing matches subscriber criteria."""
        # Price matching
        price = self._normalize_price(listing.get('asking_price'))
        if (min_price is not None and price is not None) and (price < min_price):
            return False
        if (max_price is not None and price is not None) and (price > max_price):
            return False

        # Industry matching
        if subscriber_industries:
            listing_categories = listing.get('category') or []
            if not isinstance(listing_categories, list):
                listing_categories = [listing_categories]
            listing_industries = set(str(cat).lower().strip() for cat in listing_categories if cat)
            if not subscriber_industries.intersection(listing_industries):
                return False

        # Location matching (states and cities)
        if subscriber_states:
            listing_state = set(listing.get('state', '').lower())
            if not subscriber_states.intersection(listing_state):
                return False

        if subscriber_cities:
            listing_city = set(listing.get('city', '').lower())
            if not subscriber_cities.intersection(listing_city):
                return False

        return True

    # -----------------------------
    # Grouping and Segment Management
    # -----------------------------
    def _create_listings_hash(self, listings: List[Dict]) -> str:
        """Create a unique hash for a set of listings to group identical matches."""
        # Sort listings by a unique identifier to ensure consistent hashing
        listing_ids = sorted([listing.get('url') or listing.get('title', '') for listing in listings])
        content = '|'.join(listing_ids)
        return hashlib.md5(content.encode()).hexdigest()[:8]

    def group_subscribers_by_matches(self, matches: Dict[str, List[Dict]]) -> Dict[str, Tuple[List[str], List[Dict]]]:
        """Group subscribers who have identical matched listings."""
        # Group by listing hash
        groups = {}

        for email, matched_listings in matches.items():
            listings_hash = self._create_listings_hash(matched_listings)

            if listings_hash not in groups:
                groups[listings_hash] = ([], matched_listings)

            groups[listings_hash][0].append(email)

        return groups

    def _create_segment_for_group(self, group_emails: List[str], listings_hash: str) -> Optional[str]:
        """Create a static segment for a group of subscribers with identical matches."""
        segment_name = f"alerts-group-{listings_hash}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

        try:
            # Create static segment
            response = self.mailchimp_client.lists.create_segment(
                self.list_id,
                {"name": segment_name, "static_segment": group_emails}
            )
            segment_id = response.get('id')
            logger.info(f"Created segment '{segment_name}' with {len(group_emails)} subscribers")
            return segment_id

        except ApiClientError as error:
            logger.error(f"Error creating segment for group {listings_hash}: {error.text}")
            return None

    def _send_campaign_to_segment(self, segment_id: str, listings: List[Dict], subject: str,
                                  from_name: str, reply_to: str, group_emails: list[str], group_size: int) -> bool:
        """Send a campaign to a specific segment using the template."""
        try:
            # Generate HTML for the matched listings
            listings_html = self._generate_listings_html(listings)
            listing_count = len(listings)

            # Create campaign
            campaign_payload = {
                "type": "regular",
                "recipients": {
                    "list_id": self.list_id,
                    "segment_opts": {"saved_segment_id": segment_id}
                },
                "settings": {
                    "subject_line": f"{subject} - {listing_count} New Listing{'s' if listing_count != 1 else ''}",
                    "title": f"Business Alerts Group - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ({group_size} recipients)",
                    "from_name": from_name,
                    "reply_to": reply_to,
                    "template_id": self.template_id
                }
            }

            response = self.mailchimp_client.campaigns.create(campaign_payload)
            campaign_id = response.get('id')

            existing_content = self.mailchimp_client.campaigns.get_content(campaign_id)
            template_html = existing_content.get("html", "")

            # Step B: replace placeholder
            updated_html = template_html.replace("*|TEMP_HTML|*", listings_html)
            self.mailchimp_client.campaigns.remove(campaign_id)

            campaign_payload['settings'].pop('template_id')

            print(f"new campaign_payload: {campaign_payload}")

            response = self.mailchimp_client.campaigns.create(campaign_payload)
            campaign_id = response.get('id')

            # Step C: upload updated HTML
            self.mailchimp_client.campaigns.set_content(campaign_id, {"html": updated_html})

            # Set content using template
            # content_payload = {
            #     "template": {
            #         "id": self.template_id,
            #         "sections": {
            #             "temp_html": listings_html,
            #         }
            #     }
            # }
            #
            # self.mailchimp_client.campaigns.set_content(campaign_id, content_payload)
            self.mailchimp_client.campaigns.send(campaign_id)

            logger.info(f"✅ Sent campaign {campaign_id} to segment {segment_id} ({group_size} recipients)")
            return True

        except ApiClientError as error:
            logger.error(f"❌ Campaign error for segment {segment_id}: {error.text}")
            return False

    def _cleanup_segment(self, segment_id: str) -> None:
        """Clean up segment after campaign is sent."""
        try:
            # Wait a bit before cleanup to ensure campaign is processed
            import time
            time.sleep(2)

            self.mailchimp_client.lists.delete_segment(self.list_id, segment_id)
            logger.info(f"Cleaned up segment {segment_id}")
        except ApiClientError as error:
            logger.warning(f"Could not cleanup segment {segment_id}: {error.text}")

    # -----------------------------
    # Main notification method
    # -----------------------------
    def notify(self,
               filtered_listing_details: list,
               list_id: Optional[str] = None,
               subject: str = "New BizBuySell Listings",
               from_name: str = "BizBuySell Alerts",
               reply_to: str = "trent@fcbb.com",
               cleanup_segments: bool = True) -> Dict[str, int]:
        """Send notifications by grouping subscribers with identical matches into segments."""
        if not self.api_key:
            raise RuntimeError("MAILCHIMP_API_KEY is not set.")

        list_id = list_id or self.list_id
        if not list_id:
            raise RuntimeError("MAILCHIMP_LIST_ID is not set.")

        # Get subscribers and find matches
        subscribers_response = self.fetch_subscribers()
        matches = self.match_subscribers_to_listings(
            subscribers_response.get('members', []),
            filtered_listing_details
        )

        if not matches:
            logger.info("No matched subscribers for recent listings.")
            return {"matched_subscribers": 0, "emails_sent": 0, "groups_created": 0}

        logger.info(f"Found {len(matches)} subscribers with matching listings")

        # Group subscribers by identical matches
        groups = self.group_subscribers_by_matches(matches)
        logger.info(f"Grouped subscribers into {len(groups)} segments based on identical matches")

        # Send campaigns to each group
        emails_sent = 0
        groups_processed = 0
        created_segments = []

        for listings_hash, (group_emails, group_listings) in groups.items():
            logger.info(
                f"Processing group {listings_hash}: {len(group_emails)} subscribers, {len(group_listings)} listings")

            # Create segment for this group
            segment_id = self._create_segment_for_group(group_emails, listings_hash)
            if not segment_id:
                continue

            created_segments.append(segment_id)

            # Send campaign to segment
            if self._send_campaign_to_segment(segment_id, group_listings, subject,
                                              from_name, reply_to, group_emails, len(group_emails)):
                emails_sent += len(group_emails)
                groups_processed += 1

        # Optional cleanup of segments
        # if cleanup_segments:
        #     logger.info("Cleaning up segments...")
        #     for segment_id in created_segments:
        #         self._cleanup_segment(segment_id)

        logger.info(f"Sent emails to {emails_sent} subscribers across {groups_processed} segments")
        return {
            "matched_subscribers": len(matches),
            "emails_sent": emails_sent,
            "groups_created": groups_processed,
            "success_rate": f"{(emails_sent / len(matches) * 100):.1f}%" if matches else "0%"
        }


# Backward-compatible function
def notify_subscribers(filtered_urls_details: List = [],
                       list_id: str = None,
                       subject: str = "New BizBuySell Listings",
                       from_name: str = "BizBuySell Alerts",
                       reply_to: str = "trent@fcbb.com") -> Dict[str, int]:
    notifier = MailchimpNotifier(os.getenv("MAILCHIMP_API_KEY"))
    return notifier.notify(filtered_urls_details,
                           list_id=list_id,
                           subject=subject,
                           from_name=from_name,
                           reply_to=reply_to)


if __name__ == "__main__":
    notify_subscribers(list_id="7881b0503b")

