import { NextResponse } from "next/server";
import { get, del } from "@/lib/cache";
import { query } from "@/lib/db";
import { FinalizeLogin } from "@/lib/auth";

export async function GET(request) {
  try {
    const { searchParams } = new URL(request.url);
    const token = searchParams.get('token');
    const email = await get(token);
    if (!email) {
      return NextResponse.json({ error: "Invalid token" }, { status: 400 });
    }
    await query("UPDATE ad_accounts SET verified = 1 WHERE email = ?", [email]);
    await del(token);

    const [user] = await query("SELECT * FROM ad_accounts WHERE email = ?", [email]);
    const response = await FinalizeLogin(user)
    return response;
  } catch (error) {
    console.error("Verification error:", error);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
