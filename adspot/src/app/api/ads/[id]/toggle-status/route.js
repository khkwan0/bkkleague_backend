import { NextResponse } from "next/server";
import { getSession } from "@/lib/session";
import { updateAdStatus, getAdById } from "@/lib/ads";

export async function POST(request, { params }) {
  const session = await getSession();
  if (!session) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { id } = await params;

  const { is_active } = await request.json();

  const updatedStatus = await updateAdStatus(id, is_active);
  const updatedAd = await getAdById(id);

  return NextResponse.json({status: 'ok', ad: updatedAd});
}
