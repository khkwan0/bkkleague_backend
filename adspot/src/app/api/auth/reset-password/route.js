import { NextResponse } from "next/server";
import { get, del } from "@/lib/cache";
import { query } from "@/lib/db";
import bcrypt from "bcrypt";

export async function POST(request) {
  try {
    const { token, password } = await request.json();

    // Validate input
    if (!token || !password) {
      return NextResponse.json(
        { error: "Token and password are required" },
        { status: 400 }
      );
    }

    // Get token data from Redis
    const tokenData = await get(`reset:${token}`);

    if (!tokenData) {
      return NextResponse.json(
        { error: "Invalid or expired token" },
        { status: 400 }
      );
    }

    // Hash the new password
    const saltRounds = 10;
    const hashedPassword = await bcrypt.hash(password, saltRounds);

    // Update password in database
    await query(
      "UPDATE ad_accounts SET hash = ? WHERE id = ?",
      [hashedPassword, tokenData.userId]
    );

    // Delete the reset token from Redis
    await del(`reset:${token}`);

    return NextResponse.json(
      { message: "Password reset successfully" },
      { status: 200 }
    );
  } catch (error) {
    console.error("Password reset error:", error);
    return NextResponse.json(
      { error: "Failed to reset password" },
      { status: 500 }
    );
  }
} 