"""Enable chat export for a tenant (canary / progressive rollout).

Usage:
    python -m scripts.flip_chat_export_flag --tenant-id <uuid> --enable
    python -m scripts.flip_chat_export_flag --tenant-id <uuid> --disable
"""
import argparse, asyncio
from sqlalchemy import select, update
from shared.database import async_session_factory
from shared.models import Tenant


async def main(tenant_id: str, enable: bool) -> None:
    async with async_session_factory() as s:
        t = (await s.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one()
        lj = dict(t.extra_data or {})
        limits = dict(lj.get("limits") or {})
        limits["chat_export_enabled"] = enable
        lj["limits"] = limits
        await s.execute(update(Tenant).where(Tenant.id == tenant_id).values(extra_data=lj))
        await s.commit()
        print(f"tenant={tenant_id} chat_export_enabled={enable}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenant-id", required=True)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--enable", action="store_true")
    g.add_argument("--disable", action="store_true")
    a = ap.parse_args()
    asyncio.run(main(a.tenant_id, a.enable and not a.disable))
