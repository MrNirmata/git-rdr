/** @type {import('next').NextConfig} */
const nextConfig = {
  // --- ეს არის მთავარი ცვლილება ---
  // ვთიშავთ React-ის Strict Mode-ს, რათა თავიდან ავიცილოთ ორმაგი რენდერინგი
  reactStrictMode: false,

  // API Proxy-ს ვტოვებთ, როგორც იყო
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: 'http://api:8080/:path*',
      },
    ]
  },
};

export default nextConfig;