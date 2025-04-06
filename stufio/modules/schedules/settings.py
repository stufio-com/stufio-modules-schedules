from stufio.core.setting_registry import (
    GroupMetadata,
    SubgroupMetadata,
    SettingMetadata,
    SettingType,
    settings_registry,
)

"""Register settings for this module"""
# Register a new group tab for this module
settings_registry.register_group(
    GroupMetadata(
        id="schedules",
        label="Schedules",
        icon="calendar",
        order=15,
    )
)

# Register subgroups
settings_registry.register_subgroup(
    SubgroupMetadata(
        id="general",
        group_id="schedules",
        label="General",
        order=10,
        module="schedules",
    ),
)

settings_registry.register_subgroup(
    SubgroupMetadata(
        id="advanced",
        group_id="schedules",
        label="Advanced",
        order=20,
        module="schedules",
    ),
)

# Register settings
settings_registry.register_setting(
    SettingMetadata(
        key="schedules_CHECK_INTERVAL_SECONDS",
        label="Check Interval (seconds)",
        description="How often to check for schedules that need execution",
        group="schedules",
        subgroup="general",
        type=SettingType.NUMBER,
        order=10,
        module="schedules",
        min=5,
        max=3600,
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="schedules_MAX_RETRIES",
        label="Max Retries",
        description="Maximum number of retry attempts for failed schedule executions",
        group="schedules",
        subgroup="general",
        type=SettingType.NUMBER,
        order=20,
        module="schedules",
        min=0,
        max=10,
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="schedules_RETRY_DELAY_SECONDS",
        label="Retry Delay (seconds)",
        description="Delay between retry attempts",
        group="schedules",
        subgroup="general",
        type=SettingType.NUMBER,
        order=30,
        module="schedules",
        min=1,
        max=3600,
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="schedules_EXECUTION_HISTORY_TTL_DAYS",
        label="Execution History TTL (days)",
        description="How long to keep execution history records",
        group="schedules",
        subgroup="advanced",
        type=SettingType.NUMBER,
        order=10,
        module="schedules",
        min=1,
        max=365,
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="schedules_MAX_CONCURRENT_EXECUTIONS",
        label="Max Concurrent Executions",
        description="Maximum number of schedules that can be executed simultaneously",
        group="schedules",
        subgroup="advanced",
        type=SettingType.NUMBER,
        order=20,
        module="schedules",
        min=1,
        max=100,
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="schedules_TIMEZONE",
        label="Default Timezone",
        description="Default timezone for schedules",
        group="schedules",
        subgroup="advanced",
        type=SettingType.SELECT,
        order=30,
        module="schedules",
        options=[
            {"value": "UTC", "label": "UTC"},
            {"value": "US/Eastern", "label": "US/Eastern"},
            {"value": "US/Central", "label": "US/Central"},
            {"value": "US/Mountain", "label": "US/Mountain"},
            {"value": "US/Pacific", "label": "US/Pacific"},
            {"value": "Europe/London", "label": "Europe/London"},
            {"value": "Europe/Paris", "label": "Europe/Paris"},
            {"value": "Europe/Berlin", "label": "Europe/Berlin"},
            {"value": "Asia/Tokyo", "label": "Asia/Tokyo"},
            {"value": "Asia/Shanghai", "label": "Asia/Shanghai"},
            {"value": "Australia/Sydney", "label": "Australia/Sydney"},
        ],
    )
)
