import { NextResponse } from "next/server";
import bcrypt from "bcrypt";
import { query } from "@/lib/db";
import { set } from "@/lib/cache";
import crypto from "crypto";
import { SendVerificationEmail } from "@/lib/auth";

export async function POST(request) {
  try {
    const { email, password, confirmPassword } = await request.json();

    if (password !== confirmPassword) {
      return NextResponse.json({ error: "Passwords do not match" }, { status: 400 });
    }

    // Hash the password
    const saltRounds = 10;
    const hashedPassword = await bcrypt.hash(password, saltRounds);
    const verificationToken = crypto.randomBytes(32).toString('hex');
    // Save to database
    const res = await query(
      "INSERT INTO ad_accounts (email, hash) VALUES (?, ?)",
      [email, hashedPassword]
    );
    if (res.insertId) {
      await set(verificationToken, email, 60 * 60 * 24);
    }
    await SendVerificationEmail(email, verificationToken);

    return NextResponse.json({ message: "User registered successfully" }, { status: 201 });
  } catch (error) {
    console.error("Registration error:", error);
    return NextResponse.json(
      { error: "Failed to register user" },
      { status: 500 }
    );
  }
}
