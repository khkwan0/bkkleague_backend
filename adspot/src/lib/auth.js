import { NextResponse } from "next/server";
import { set } from "@/lib/cache";
import crypto from "crypto";
import { sendEmail } from "@/lib/email";
import bcrypt from "bcrypt";
import { query } from "@/lib/db";

export async function FinalizeLogin(user, rememberMe = false) {
  const token = crypto.randomBytes(64).toString('hex');
  const sessionExpiry = rememberMe ? 30 * 24 * 60 * 60 : 24 * 60 * 60;
  await set(`session:${token}`, { userId: user.id }, sessionExpiry);
  const response = NextResponse.json(
    {
      message: "Login successful",
    },
    {
      status: 302,
      headers: {
        Location: `https://${process.env.NEXT_PUBLIC_DOMAIN}/dashboard`,
      },
    }
  )
  response.cookies.set('session', token, {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: sessionExpiry,
    path: '/',
  });

  return response;
}

export async function SendVerificationEmail(email, token) {
  try {
    const verificationLink = `${process.env.NEXT_PUBLIC_DOMAIN}/api/auth/verify?token=${token}`;
    const emailContent = `
    <p>Hello,</p>
    <p>Please click the link below to verify your email:</p>
    <a href="${verificationLink}">Verify Email</a>
  `;

  await sendEmail(email, 'Verify Your Email', emailContent);
  } catch (error) {
    console.error("Email sending error:", error);
    throw new Error("Failed to send verification email");
  }
}

export async function ChangePassword(userId, currentPassword, newPassword) {
  try {
    const [user] = await query(
      "SELECT * FROM ad_accounts WHERE id = ?",
      [userId]
    );

    if (!user) {
      throw new Error("User not found");
    }

    // Verify current password
    const isValidPassword = await bcrypt.compare(currentPassword, user.hash);
    if (!isValidPassword) {
      throw new Error("Current password is incorrect");
    }

    // Hash new password
    const salt = await bcrypt.genSalt(10);
    const newHash = await bcrypt.hash(newPassword, salt);

    // Update password in database
    await query(
      "UPDATE ad_accounts SET hash = ? WHERE id = ?",
      [newHash, userId]
    );

    return { success: true };
  } catch (error) {
    console.error("Password change error:", error);
    throw error;
  }
}
