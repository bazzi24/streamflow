from enum import IntEnum
from strenum import StrEnum

class UserTenantRole(StrEnum):
    OWNER = 'owner'
    ADMIN = "admin"
    NORMAL = 'normal'
    INVITE = 'invite'

class TenantPermission(StrEnum):
    ME = 'me'
    TEAM = 'team'

