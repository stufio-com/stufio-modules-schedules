"""
Examples of Periodical Events Definition for Stufio Schedules Module

This file demonstrates various ways to define scheduled events that run periodically
based on cron expressions. These events are automatically registered and synced to 
MongoDB on application startup.
"""

from datetime import datetime, timedelta
from typing import Dict, Any
from stufio.modules.events.schemas.payloads import BaseEventPayload
from stufio.modules.schedules.schemas.scheduled_event_definition import ScheduledEventDefinition


# =============================================================================
# BASIC EXAMPLES
# =============================================================================

class DailyCleanupScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Daily cleanup of temporary files and expired data."""
    entity_type = "system"
    action = "cleanup"
    description = "Daily cleanup of temporary files and expired data"
    cron_expression = "0 2 * * *"  # Every day at 2:00 AM
    payload = {"cleanup_type": "daily", "cleanup_targets": ["temp_files", "expired_sessions"]}


class WeeklyReportScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Weekly analytics report generation."""
    entity_type = "report"
    action = "generate"
    description = "Generate weekly analytics report for administrators"
    cron_expression = "0 8 * * 1"  # Every Monday at 8:00 AM
    payload = {"report_type": "weekly", "recipients": ["admin@example.com"]}


class MonthlyBillingScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Monthly billing cycle processing."""
    entity_type = "billing"
    action = "process"
    description = "Process monthly billing for all active subscriptions"
    cron_expression = "0 0 1 * *"  # First day of every month at midnight
    payload = {"billing_type": "monthly", "include_prorations": True}


# =============================================================================
# USER MANAGEMENT EXAMPLES
# =============================================================================

class InactiveUserReminderScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Send reminders to inactive users."""
    entity_type = "user"
    action = "send_reminder"
    description = "Send email reminders to users who haven't logged in for 30 days"
    cron_expression = "0 10 * * *"  # Every day at 10:00 AM
    payload = {
        "reminder_type": "inactive_user",
        "inactive_days_threshold": 30,
        "email_template": "user_activity_reminder"
    }


class ExpiredSubscriptionCleanupScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Clean up expired subscriptions."""
    entity_type = "subscription"
    action = "cleanup"
    description = "Archive expired subscriptions and notify users"
    cron_expression = "0 3 * * *"  # Every day at 3:00 AM
    payload = {
        "grace_period_days": 7,
        "send_notification": True,
        "archive_after_days": 30
    }


# =============================================================================
# CONTENT MANAGEMENT EXAMPLES
# =============================================================================

class ContentModerationScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Automated content moderation check."""
    entity_type = "content"
    action = "moderate"
    description = "Run automated moderation on new content submissions"
    cron_expression = "*/30 * * * *"  # Every 30 minutes
    payload = {
        "moderation_type": "automated",
        "check_types": ["spam", "inappropriate", "copyright"],
        "confidence_threshold": 0.8
    }


class SitemapGenerationScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Generate sitemap for SEO."""
    entity_type = "seo"
    action = "generate_sitemap"
    description = "Generate XML sitemap for search engines"
    cron_expression = "0 4 * * *"  # Every day at 4:00 AM
    payload = {
        "include_images": True,
        "include_videos": True,
        "max_urls_per_file": 50000
    }


# =============================================================================
# BACKUP AND MAINTENANCE EXAMPLES
# =============================================================================

class DatabaseBackupScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Daily database backup."""
    entity_type = "database"
    action = "backup"
    description = "Create daily backup of critical database tables"
    cron_expression = "0 1 * * *"  # Every day at 1:00 AM
    payload = {
        "backup_type": "incremental",
        "retention_days": 30,
        "compress": True,
        "tables": ["users", "orders", "payments"]
    }


class LogRotationScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Weekly log file rotation."""
    entity_type = "logs"
    action = "rotate"
    description = "Rotate and compress log files to manage disk space"
    cron_expression = "0 0 * * 0"  # Every Sunday at midnight
    payload = {
        "max_file_size": "100MB",
        "keep_files": 12,
        "compress": True,
        "log_types": ["application", "access", "error"]
    }


# =============================================================================
# ANALYTICS AND METRICS EXAMPLES
# =============================================================================

class HourlyMetricsScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Collect hourly system metrics."""
    entity_type = "metrics"
    action = "collect"
    description = "Collect and aggregate hourly system performance metrics"
    cron_expression = "0 * * * *"  # Every hour at minute 0
    payload = {
        "metrics_types": ["cpu", "memory", "disk", "network"],
        "aggregation": "hourly",
        "store_raw": False
    }


class DailyAnalyticsScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Generate daily analytics reports."""
    entity_type = "analytics"
    action = "generate_report"
    description = "Generate daily analytics and usage reports"
    cron_expression = "0 6 * * *"  # Every day at 6:00 AM
    payload = {
        "report_types": ["user_activity", "revenue", "performance"],
        "date_range": "yesterday",
        "format": "json",
        "email_summary": True
    }


# =============================================================================
# NOTIFICATION EXAMPLES
# =============================================================================

class WeeklyNewsletterScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Send weekly newsletter to subscribers."""
    entity_type = "newsletter"
    action = "send"
    description = "Send weekly newsletter to all subscribed users"
    cron_expression = "0 9 * * 2"  # Every Tuesday at 9:00 AM
    payload = {
        "newsletter_type": "weekly",
        "template": "weekly_digest",
        "personalized": True,
        "track_opens": True
    }


class MaintenanceNotificationScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Send maintenance notifications."""
    entity_type = "maintenance"
    action = "notify"
    description = "Send scheduled maintenance notifications to users"
    cron_expression = "0 20 * * 5"  # Every Friday at 8:00 PM
    payload = {
        "notification_type": "maintenance",
        "advance_notice_hours": 48,
        "maintenance_window": "weekend",
        "channels": ["email", "in_app", "sms"]
    }


# =============================================================================
# E-COMMERCE EXAMPLES
# =============================================================================

class CartAbandonmentReminderScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Send cart abandonment reminders."""
    entity_type = "cart"
    action = "send_reminder"
    description = "Send reminders for abandoned shopping carts"
    cron_expression = "0 */6 * * *"  # Every 6 hours
    payload = {
        "reminder_type": "cart_abandonment",
        "abandoned_hours_threshold": 24,
        "max_reminders": 3,
        "discount_code": "COMEBACK10"
    }


class InventoryLowStockScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Check for low stock items."""
    entity_type = "inventory"
    action = "check_stock"
    description = "Check inventory levels and alert for low stock items"
    cron_expression = "0 */4 * * *"  # Every 4 hours
    payload = {
        "low_stock_threshold": 10,
        "alert_channels": ["email", "slack"],
        "auto_reorder": False,
        "priority_items": ["best_sellers", "seasonal"]
    }


# =============================================================================
# SECURITY EXAMPLES
# =============================================================================

class SecurityScanScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Automated security vulnerability scan."""
    entity_type = "security"
    action = "scan"
    description = "Run automated security scans on the application"
    cron_expression = "0 23 * * *"  # Every day at 11:00 PM
    payload = {
        "scan_types": ["vulnerability", "dependency", "config"],
        "severity_threshold": "medium",
        "alert_on_new": True,
        "exclude_paths": ["/test", "/docs"]
    }


class SuspiciousActivityCheckScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Check for suspicious user activity."""
    entity_type = "security"
    action = "check_activity"
    description = "Analyze user activity patterns for suspicious behavior"
    cron_expression = "*/15 * * * *"  # Every 15 minutes
    payload = {
        "analysis_window_hours": 1,
        "flags": ["multiple_logins", "unusual_location", "high_request_volume"],
        "auto_block": False,
        "alert_threshold": 0.7
    }


# =============================================================================
# ADVANCED CRON EXPRESSIONS EXAMPLES
# =============================================================================

class QuarterlyComplianceScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Quarterly compliance report generation."""
    entity_type = "compliance"
    action = "generate_report"
    description = "Generate quarterly compliance reports for regulatory requirements"
    cron_expression = "0 9 1 */3 *"  # First day of every quarter at 9:00 AM
    payload = {
        "report_type": "quarterly_compliance",
        "regulations": ["GDPR", "SOX", "PCI_DSS"],
        "audit_trail": True
    }


class BusinessHoursHealthCheckScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Health checks during business hours only."""
    entity_type = "system"
    action = "health_check"
    description = "Perform detailed health checks during business hours"
    cron_expression = "*/5 9-17 * * 1-5"  # Every 5 minutes, 9 AM to 5 PM, Monday to Friday
    payload = {
        "check_types": ["api_response", "database", "external_services"],
        "timeout_seconds": 30,
        "retry_count": 3
    }


class WeekendMaintenanceScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Weekend maintenance tasks."""
    entity_type = "maintenance"
    action = "perform"
    description = "Perform intensive maintenance tasks during weekends"
    cron_expression = "0 2 * * 6,0"  # 2:00 AM on Saturdays and Sundays
    payload = {
        "maintenance_type": "intensive",
        "tasks": ["index_optimization", "cache_rebuild", "log_analysis"],
        "max_duration_hours": 4
    }


# =============================================================================
# CUSTOM TIMEZONE EXAMPLES
# =============================================================================

class EuropeanMarketOpenScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """European market opening tasks."""
    entity_type = "market"
    action = "prepare"
    description = "Prepare systems for European market opening"
    cron_expression = "0 8 * * 1-5"  # 8:00 AM, Monday to Friday
    timezone = "Europe/London"
    payload = {
        "market": "european",
        "preparation_tasks": ["update_rates", "sync_data", "check_systems"]
    }


class AsianMarketReportScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Asian market daily report."""
    entity_type = "market"
    action = "generate_report"
    description = "Generate daily market report for Asian markets"
    cron_expression = "0 18 * * 1-5"  # 6:00 PM, Monday to Friday
    timezone = "Asia/Tokyo"
    payload = {
        "market": "asian",
        "report_type": "daily_summary",
        "include_futures": True
    }


# =============================================================================
# HIGH FREQUENCY EXAMPLES
# =============================================================================

class SystemMonitoringScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """High-frequency system monitoring."""
    entity_type = "monitoring"
    action = "check"
    description = "High-frequency system health monitoring"
    cron_expression = "* * * * *"  # Every minute
    payload = {
        "monitoring_type": "realtime",
        "checks": ["memory", "cpu", "disk"],
        "alert_threshold": 90
    }


class ApiRateLimitResetScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Reset API rate limits."""
    entity_type = "api"
    action = "reset_limits"
    description = "Reset API rate limits for all users"
    cron_expression = "0 * * * *"  # Every hour
    payload = {
        "limit_type": "hourly",
        "reset_all": True,
        "log_usage": True
    }


# =============================================================================
# CONDITIONAL/COMPLEX EXAMPLES
# =============================================================================

class SeasonalCampaignScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Seasonal marketing campaign trigger."""
    entity_type = "marketing"
    action = "trigger_campaign"
    description = "Trigger seasonal marketing campaigns based on calendar"
    cron_expression = "0 10 1 3,6,9,12 *"  # First day of Mar, Jun, Sep, Dec at 10:00 AM
    payload = {
        "campaign_type": "seasonal",
        "auto_adjust_content": True,
        "target_segments": ["active_users", "premium_users"]
    }


class WorkdayEndScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """End of workday cleanup and reporting."""
    entity_type = "workday"
    action = "end_day"
    description = "Perform end-of-workday cleanup and generate daily reports"
    cron_expression = "0 18 * * 1-5"  # 6:00 PM, Monday to Friday
    payload = {
        "cleanup_tasks": ["temp_files", "session_cleanup"],
        "generate_reports": ["daily_activity", "performance"],
        "notify_managers": True
    }


# =============================================================================
# INTEGRATION EXAMPLES
# =============================================================================

class ThirdPartyDataSyncScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Sync data with third-party services."""
    entity_type = "integration"
    action = "sync_data"
    description = "Synchronize data with external third-party services"
    cron_expression = "0 */2 * * *"  # Every 2 hours
    payload = {
        "services": ["crm", "analytics", "payment_processor"],
        "sync_type": "incremental",
        "error_tolerance": "medium"
    }


class WebhookHealthCheckScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
    """Check health of outgoing webhooks."""
    entity_type = "webhook"
    action = "health_check"
    description = "Check the health and responsiveness of configured webhooks"
    cron_expression = "*/10 * * * *"  # Every 10 minutes
    payload = {
        "timeout_seconds": 10,
        "retry_failed": True,
        "disable_after_failures": 5
    }


# =============================================================================
# USAGE EXAMPLES
# =============================================================================

def example_usage():
    """
    Example of how these scheduled events would be used in practice.
    
    The events defined above are automatically registered when the module loads.
    They are then synced to MongoDB and executed according to their schedules.
    """
    
    # All events are automatically registered through the metaclass
    # You can get all registered events like this:
    from stufio.modules.schedules.schemas.scheduled_event_definition import get_all_scheduled_events
    
    all_events = get_all_scheduled_events()
    print(f"Registered {len(all_events)} scheduled events:")
    
    for event_class in all_events:
        print(f"- {event_class.__name__}: {event_class.get_description()}")
        print(f"  Schedule: {event_class.get_cron_expression()}")
        print(f"  Entity: {event_class.get_entity_type()}.{event_class.get_action()}")
        print()


if __name__ == "__main__":
    example_usage()
