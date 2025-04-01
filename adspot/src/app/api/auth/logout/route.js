import { NextResponse } from "next/server";
import { cookies } from 'next/headers';
import { del } from "@/lib/cache";

export async function POST() {
  try {
    const cookieStore = await cookies();
    const sessionToken = cookieStore?.get('session')?.value;

    if (sessionToken) {
      // Delete the session from Redis
      await del(`session:${sessionToken}`);
    }

    // Create response
    const response = NextResponse.json(
      { message: "Logged out successfully" },
      { status: 200 }
    );

    // Clear the session cookie
    response.cookies.delete('session');

    return response;
  } catch (error) {
    console.error("Logout error:", error);
    return NextResponse.json(
      { error: "Failed to logout" },
      { status: 500 }
    );
  }
} 