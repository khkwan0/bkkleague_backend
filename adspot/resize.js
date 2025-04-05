import sharp from 'sharp'

sharp('./ad_original.png').resize(327,327).toFile('./ad.png', (err, info) => {})
