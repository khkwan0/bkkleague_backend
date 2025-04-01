import { NextResponse } from "next/server";
import bcrypt from "bcrypt";
import { query } from "@/lib/db";
import { FinalizeLogin } from "@/lib/auth";

export async function POST(request) {
  try {
    const { email, password, rememberMe } = await request.json();

    // Validate input
    if (!email || !password) {
      return NextResponse.json(
        { error: "Email and password are required" },
        { status: 400 }
      );
    }

    // Get user from database
    const [user] = await query(
      "SELECT * FROM ad_accounts WHERE email = ? and verified = 1",
      [email]
    );

    if (!user) {
      return NextResponse.json(
        { error: "Invalid email or password" },
        { status: 401 }
      );
    }

    // Compare password with stored hash
    const isValidPassword = await bcrypt.compare(password, user.hash);

    if (!isValidPassword) {
      return NextResponse.json(
        { error: "Invalid email or password" },
        { status: 401 }
      );
    }

    const response = await FinalizeLogin(user, rememberMe)
    return response;
  } catch (error) {
    console.error("Login error:", error);
    return NextResponse.json(
      { error: "Internal server error" },
      { status: 500 }
    );
  }
}
