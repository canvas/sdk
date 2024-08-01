import type { Config } from "tailwindcss";
import defaultTheme from "tailwindcss/defaultTheme";
import colors from "tailwindcss/colors";

const disabledCss = {
  "code::before": false,
  "code::after": false,
};

const config: Config = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/content/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  safelist: [{ pattern: /theme-.+/ }],
  theme: {
    fontFamily: {
      sans: ["Inter", ...defaultTheme.fontFamily.sans],
      display: ["var(--font-twklausanne)", ...defaultTheme.fontFamily.sans],
      code: ["var(--font-inconsolata)", ...defaultTheme.fontFamily.sans],
    },
    extend: {
      backgroundImage: {
        "gradient-radial": "radial-gradient(var(--tw-gradient-stops))",
        "gradient-conic":
          "conic-gradient(from 180deg at 50% 50%, var(--tw-gradient-stops))",
      },
      boxShadow: {
        box: "0 1px 4px 0 rgba(13,34,71,0.07)",
        "box-md": "0 4px 8px -1px rgba(13,34,71,0.07)",
      },
      colors: {
        transparent: colors.transparent,
        inherit: colors.inherit,
        current: colors.current,
        white: colors.white,
        black: colors.black,
        "canvas-blue": {
          "020": "#f7f8fa",
          "050": "#e7e9ed",
          100: "#cfd3da",
          200: "#b6bdc8",
          300: "#9ea7b5",
          400: "#8690a3",
          500: "#6e7a91",
          600: "#56647e",
          700: "#3d4e6c",
          800: "#253859",
          900: "#0d2247",
        },
        marble: {
          50: "#F7F7F7",
          100: "#f2f2f2",
          200: "#e5e5e6",
          300: "#d8d9d9",
          400: "#c8c9ca",
          500: "#bbbcbe",
          600: "#afb0b2",
          700: "#828487",
          800: "#58595b",
          900: "#2b2b2c",
          950: "#171717",
        },
        background: "#edeff4",
        primary: "var(--primary)",
        "gradient-start": "var(--gradient-start)",
        "gradient-end": "var(--gradient-end)",
      },
      typography: {
        DEFAULT: { css: disabledCss },
        sm: { css: disabledCss },
        lg: { css: disabledCss },
        xl: { css: disabledCss },
        "2xl": { css: disabledCss },
      },
    },
  },
  plugins: [require("@tailwindcss/typography")],
};
export default config;
