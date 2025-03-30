export default function Home() {
  return (
    <main className="min-h-screen flex items-center justify-center relative px-4">
      {/* Background Image */}
      <div 
        className="absolute inset-0 z-0"
        style={{
          backgroundImage: 'url("/adspot_image5.png")',
          backgroundSize: 'cover',
          backgroundPosition: 'center',
          backgroundRepeat: 'no-repeat',
        }}
      />
      
      {/* Overlay for better text visibility */}
      <div className="absolute inset-0 bg-black/50 z-10" />
      
      {/* Login Link */}
      <a 
        href="/auth/login" 
        className="absolute top-4 right-4 sm:top-6 sm:right-6 z-20 px-4 sm:px-6 py-2 rounded-full bg-white/10 hover:bg-white/20 text-white text-sm sm:text-base font-medium transition-all duration-300 backdrop-blur-sm border border-white/20 hover:border-white/30"
      >
        Login
      </a>
      
      {/* Content */}
      <div className="text-center relative z-20 max-w-[90vw] sm:max-w-none">
        <h1 className="text-4xl sm:text-5xl md:text-7xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-blue-500 via-purple-500 to-pink-500 animate-gradient">
          Adspot
        </h1>
        <p className="mt-3 sm:mt-4 text-gray-200 text-base sm:text-lg md:text-xl max-w-sm mx-auto">
          Your Digital Advertising Hub
        </p>
      </div>
    </main>
  )
}
