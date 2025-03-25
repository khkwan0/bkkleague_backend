import { getSession } from "@/lib/session";
import { redirect } from "next/navigation";
import DashboardComponent from "@/components/dashboard";

export default async function Dashboard() {
  const session = await getSession();
  if (!session) {
    redirect('/auth/login');
  }
  return (
    <DashboardComponent />
  );
}