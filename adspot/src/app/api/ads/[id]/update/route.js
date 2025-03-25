import { NextResponse } from "next/server";
import { getSession } from "@/lib/session";
import { query } from "@/lib/db";
import { getAdById } from "@/lib/ads";

export async function PUT(request, { params }) {
  const session = await getSession();
  if (!session) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const { id } = await params;
    const { title, message, click_url } = await request.json();

    // Get ad details to verify ownership
    const ad = await getAdById(id);
    if (!ad || ad.account_id !== session.userId) {
      return NextResponse.json({ error: "Ad not found or unauthorized" }, { status: 404 });
    }

    // Update ad details
    await query(
      'UPDATE ad_spots SET title = ?, message = ?, click_url = ? WHERE id = ?',
      [title, message, click_url, id]
    );

    // Get updated ad
    const updatedAd = await getAdById(id);
    return NextResponse.json({ status: 'ok', ad: updatedAd });
  } catch (error) {
    console.error('Error updating ad:', error);
    return NextResponse.json({ error: "Failed to update ad" }, { status: 500 });
  }
} 