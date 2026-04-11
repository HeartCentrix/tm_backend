"""Multi-App Registration Manager for Microsoft Graph API
Distributes requests across multiple app registrations to avoid throttling.
"""
import time
import threading
import hashlib
from typing import Dict, List, Optional
from shared.config import settings


class AppRegistry:
    """Tracks usage of a single Graph app registration"""
    def __init__(self, app: dict):
        self.index = app["index"]
        self.client_id = app["client_id"]
        self.client_secret = app["client_secret"]
        self.tenant_id = app["tenant_id"]
        self.request_count = 0
        self.last_request_time = 0.0
        self.throttled_until = 0.0

    @property
    def is_throttled(self) -> bool:
        return time.time() < self.throttled_until

    @property
    def load_score(self) -> float:
        """Lower score = less loaded = better choice"""
        if self.is_throttled:
            return float('inf')
        return self.request_count


class MultiAppManager:
    """Manages multiple Graph app registrations with round-robin + load balancing"""

    def __init__(self):
        self.apps: List[AppRegistry] = [
            AppRegistry(app) for app in settings.GRAPH_APPS
        ]
        self._current_index = 0
        self._lock = threading.Lock()
        self._app_map: Dict[str, AppRegistry] = {
            app.client_id: app for app in self.apps
        }

    @property
    def app_count(self) -> int:
        return len(self.apps)

    def get_next_app(self) -> AppRegistry:
        """Get the next app using round-robin with throttling awareness.
        Thread-safe via lock for use across threads and async contexts."""
        if len(self.apps) == 1:
            app = self.apps[0]
            app.request_count += 1
            app.last_request_time = time.time()
            return app

        with self._lock:
            # Try round-robin first, skipping throttled apps
            for _ in range(len(self.apps)):
                app = self.apps[self._current_index % len(self.apps)]
                self._current_index += 1
                if not app.is_throttled:
                    app.request_count += 1
                    app.last_request_time = time.time()
                    return app

            # All apps throttled, return least loaded
            return min(self.apps, key=lambda a: a.load_score)

    def get_app_by_client_id(self, client_id: str) -> Optional[AppRegistry]:
        """Get specific app by client_id"""
        return self._app_map.get(client_id)

    def mark_throttled(self, client_id: str, retry_after_seconds: int):
        """Mark an app as throttled (thread-safe)"""
        with self._lock:
            app = self._app_map.get(client_id)
            if app:
                app.throttled_until = time.time() + retry_after_seconds

    def reset_throttle(self, client_id: str):
        """Reset throttle state for an app (thread-safe)"""
        with self._lock:
            app = self._app_map.get(client_id)
            if app:
                app.throttled_until = 0.0

    def get_stats(self) -> List[dict]:
        """Get usage stats for all apps"""
        return [
            {
                "index": app.index,
                "client_id": app.client_id,
                "request_count": app.request_count,
                "is_throttled": app.is_throttled,
                "throttled_until": app.throttled_until,
            }
            for app in self.apps
        ]


# Global instance
multi_app_manager = MultiAppManager()
