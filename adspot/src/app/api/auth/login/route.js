import { NextResponse } from "next/server";
import bcrypt from "bcrypt";
import { query } from "@/lib/db";
import { set } from "@/lib/cache";
import crypto from "crypto";

export async function POST(request) {
  try {
    const { email, password } = await request.json();

    // Validate input
    if (!email || !password) {
      return NextResponse.json(
        { error: "Email and password are required" },
        { status: 400 }
      );
    }

    // Get user from database
    const [user] = await query(
      "SELECT * FROM ad_accounts WHERE email = ?",
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

    // Generate a random 64-byte token
    const token = crypto.randomBytes(64).toString('hex');

    // Store token in Redis with 24 hour expiry
    await set(`session:${token}`, { userId: user.id }, 24 * 60 * 60);

    // Remove password from user object before sending response
    const { password: _, ...userWithoutPassword } = user;

    // Create response with user data
    const response = NextResponse.json({
      message: "Login successful",
      user: userWithoutPassword,
    });

    // Set the token as an HTTP-only cookie
    response.cookies.set('session', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 24 * 60 * 60, // 24 hours
      path: '/',
    });

    return response;
  } catch (error) {
    console.error("Login error:", error);
    return NextResponse.json(
      { error: "Internal server error" },
      { status: 500 }
    );
  }
}
