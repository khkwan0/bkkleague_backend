import { NextResponse } from "next/server";
import { getSession } from "@/lib/session";
import { getAdsByAccountId, createAd } from "@/lib/ads";

export async function GET(request) {
  const session = await getSession();
  if (!session) { 
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const ads = await getAdsByAccountId(session.userId);
  return NextResponse.json(ads);
}

export async function POST(request) {
  try {
    const session = await getSession();
    if (!session) { 
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const body = await request.json();
    const { title, message, click_url, is_active } = body;

    if (!title || !click_url) {
      return NextResponse.json({ error: "Title and click_url are required" }, { status: 400 });
    }

    const adId = await createAd(session.userId, title, message, click_url, is_active ?? true);
    return NextResponse.json({ adId: adId });
  } catch (error) {
    console.error(error);
    return NextResponse.json({ error: "Internal Server Error" }, { status: 500 });
  }
}
