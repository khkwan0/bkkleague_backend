import { NextResponse } from "next/server";
import { getSession } from "@/lib/session";
import { mkdir, writeFile } from 'fs/promises';
import { join } from 'path';
import { getAdById } from "@/lib/ads";
import { query } from "@/lib/db";

export async function POST(request, { params }) {
  const session = await getSession();
  if (!session) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const { id } = await params;
    const formData = await request.formData();
    const file = formData.get('image');

    if (!file) {
      return NextResponse.json({ error: "No image file provided" }, { status: 400 });
    }

    // Validate file type
    if (!file.type.startsWith('image/')) {
      return NextResponse.json({ error: "File must be an image" }, { status: 400 });
    }

    // Get ad details to verify ownership
    const ad = await getAdById(id);
    if (!ad || ad.account_id !== session.userId) {
      return NextResponse.json({ error: "Ad not found or unauthorized" }, { status: 404 });
    }

    // Create directory if it doesn't exist
    const uploadDir = join(process.cwd(), 'public', 'ads', ad.account_id.toString(), id.toString());
    
    // Create directory if it doesn't exist
    try {
      await mkdir(uploadDir, { recursive: true });
    } catch (err) {
    }
    await writeFile(join(uploadDir, 'ad.png'), Buffer.from(await file.arrayBuffer()));

    // Update the background column in ad_spots table
    const prefix = 'https://' + process.env.NEXT_PUBLIC_DOMAIN + '/ads/' + ad.account_id.toString() + '/' + id.toString() + '/';
    await query(
      'UPDATE ad_spots SET background = ? WHERE id = ?',
      [prefix + 'ad.png', id]
    );

    return NextResponse.json({ status: 'ok', message: 'Image updated successfully' });
  } catch (error) {
    console.error('Error updating ad image:', error);
    return NextResponse.json({ error: "Failed to update image" }, { status: 500 });
  }
} 