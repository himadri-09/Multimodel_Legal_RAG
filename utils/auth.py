# utils/auth.py
from typing import Optional, Dict
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from supabase import create_client, Client

from config import SUPABASE_URL, SUPABASE_SERVICE_KEY

# HTTP Bearer security scheme
security = HTTPBearer()

# Supabase client singleton (using SERVICE ROLE KEY for backend operations)
_supabase_client: Optional[Client] = None


def get_supabase_client() -> Client:
    """
    Get or create Supabase client singleton with SERVICE ROLE KEY

    This bypasses Row Level Security (RLS) for backend operations.
    The backend still validates user JWT tokens via get_current_user().

    Returns:
        Client: Supabase client instance with service role permissions
    """
    global _supabase_client

    if _supabase_client is None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in environment variables")

        _supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        print("✅ Supabase client initialized with SERVICE ROLE KEY")

    return _supabase_client


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict:
    """
    FastAPI dependency to extract and validate current user from Bearer token

    Uses Supabase's built-in token validation - much simpler!

    Args:
        credentials: HTTP Authorization credentials containing Bearer token

    Returns:
        Dict: User information with 'id' and 'email' fields

    Raises:
        HTTPException: If token is invalid, expired, or missing required fields
    """
    token = credentials.credentials

    try:
        # Use Supabase to validate the token
        supabase = get_supabase_client()

        # Get user from token - Supabase handles all validation
        user_response = supabase.auth.get_user(token)

        if not user_response or not user_response.user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user = user_response.user

        # Return user context
        return {
            "id": user.id,
            "email": user.email or "",
            "user_metadata": user.user_metadata or {}
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate credentials: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_active_user(
    current_user: Dict = Depends(get_current_user)
) -> Dict:
    """
    FastAPI dependency to get current active user (with additional checks if needed)

    Args:
        current_user: User dict from get_current_user dependency

    Returns:
        Dict: Active user information

    Raises:
        HTTPException: If user is inactive or disabled
    """
    # In the future, you could add additional checks here:
    # - Check if user is disabled/banned
    # - Check if user has verified email
    # - Check subscription status, etc.

    # For now, just return the user as-is
    return current_user
