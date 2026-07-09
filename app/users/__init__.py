from app.users.profile_support import (
    ensure_service_role_configured,
    execute_supabase,
    get_cached_user_id_from_bearer,
    get_location_path_v2,
    is_profile_locked,
    require_can_edit_self,
    safe_supabase_data,
)
