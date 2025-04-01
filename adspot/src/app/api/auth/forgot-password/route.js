import { NextResponse } from "next/server";
import { query } from "@/lib/db";
import { set } from "@/lib/cache";
import crypto from "crypto";
import { SendForgotPasswordEmail } from "@/lib/auth";

export async function POST(request) {
  try {
    const { email } = await request.json();

    // Validate input
    if (!email) {
      return NextResponse.json(
        { error: "Email is required" },
        { status: 400 }
      );
    }

    // Get user from database
    const [user] = await query(
      "SELECT * FROM ad_accounts WHERE email = ?",
      [email]
    );

    if (!user) {
      // Return success even if user doesn't exist to prevent email enumeration
      return NextResponse.json(
        { message: "If an account exists with this email, you will receive a password reset link." },
        { status: 200 }
      );
    }

    // Generate a random token
    const token = crypto.randomBytes(32).toString('hex');

    // Store token in Redis with 1 hour expiry
    await set(`reset:${token}`, { userId: user.id }, 60 * 60);

    // Create reset link
    const resetLink = `${process.env.NEXT_PUBLIC_DOMAIN}/auth/reset-password?token=${token}`;

    // Send email
    await SendForgotPasswordEmail(email, resetLink);

    return NextResponse.json(
      { message: "If an account exists with this email, you will receive a password reset link." },
      { status: 200 }
    );
  } catch (error) {
    console.error("Forgot password error:", error);
    return NextResponse.json(
      { error: "Failed to process request" },
      { status: 500 }
    );
  }
} 