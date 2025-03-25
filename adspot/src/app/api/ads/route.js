import { NextResponse } from "next/server";
import { getSession } from "@/lib/session";
import { getAdsByAccountId } from "@/lib/ads";

export async function GET(request) {
  const session = await getSession();
  if (!session) { 
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const ads = await getAdsByAccountId(session.userId);
  return NextResponse.json(ads);
}