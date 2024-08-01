"use server";

import { cookies } from "next/headers";

export async function setLanguage(language: string) {
  cookies().set("language", language);
}
