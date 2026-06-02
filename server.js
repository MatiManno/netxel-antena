const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

app.use(express.static('public'));

let usuariosConectados = {}; 
let contadorUsuarios = 0;

// Objeto para controlar el estado de las conversaciones en tiempo real
// Estructura: { "id_emisor-id_receptor": timeoutID }
let sesionesCanalAbierto = {};

io.on('connection', (socket) => {
    console.log(`🔌 Conexión a la antena: ${socket.id}`);

    socket.on('unirse_flota', (data) => {
        contadorUsuarios++;
        const idNextelGenerado = `${data.flota}*${contadorUsuarios}`;
        
        socket.idNextel = idNextelGenerado;
        socket.flotaNextel = data.flota;
        socket.nombreNextel = data.nombre;
        usuariosConectados[idNextelGenerado] = socket.id;

        console.log(`📟 Radio Activa: ${data.nombre} -> ID ${idNextelGenerado}`);
        socket.emit('conexion_exitosa', { idNextel: idNextelGenerado, mensaje: "NetXel Activa" });
    });

    socket.on('enviar_alerta', (data) => {
        const receptorSocketId = usuariosConectados[data.destino];
        if (receptorSocketId) {
            io.to(receptorSocketId).emit('recibir_alerta', {
                desde: socket.idNextel,
                nombre: socket.nombreNextel
            });
        } else {
            socket.emit('error_conexion', { mensaje: "ID fuera de área." });
        }
    });

    // 🎙️ EN TIEMPO REAL: RECIBIR TROZOS DE AUDIO EN VIVO Y TRANSMITIRLOS AL INSTANTE
    socket.on('stream_audio_vivo', (data) => {
        const receptorSocketId = usuariosConectados[data.destino];
        if (!receptorSocketId) return;

        // Generamos una clave única para identificar la sesión entre estos dos handies
        const parRadios = [socket.idNextel, data.destino].sort().join('-');

        let debeSonarChirp = false;

        // Si NO existe una sesión activa para este par, significa que el canal estaba CERRADO
        if (!sesionesCanalAbierto[parRadios]) {
            debeSonarChirp = true; // Hay que avisarle al receptor que haga sonar el bip-bip de apertura
            console.log(`📡 [Abriendo Canal] Conversación nueva entre ${socket.idNextel} y ${data.destino}`);
        } else {
            // Si ya existía, cancelamos el temporizador de cierre viejo porque siguen hablando
            clearTimeout(sesionesCanalAbierto[parRadios]);
        }

        // Seteamos (o renovamos) el temporizador a 12 SEGUNDOS CLAVADOS de canal abierto
        sesionesCanalAbierto[parRadios] = setTimeout(() => {
            console.log(`⏳ [Canal Cerrado] Se cumplieron los 12 segundos de inactividad entre ${socket.idNextel} y ${data.destino}`);
            delete sesionesCanalAbierto[parRadios];
            
            // Le avisamos a ambas radios que el canal se cayó por inactividad para que vuelvan a READY
            io.to(socket.id).emit('canal_cerrado_inactividad');
            io.to(receptorSocketId).emit('canal_cerrado_inactividad');
        }, 12000);

        // Le retransmitimos el pedacito de voz en vivo al receptor al milisegundo
        io.to(receptorSocketId).emit('recibir_audio_vivo', {
            chunk: data.chunk, // El fragmento diminuto de voz
            desde: socket.idNextel,
            nombre: socket.nombreNextel,
            abrirCanal: debeSonarChirp // Le indica si tiene que ejecutar el sonido inicial
        });
    });

    socket.on('disconnect', () => {
        if (socket.idNextel) {
            console.log(`🛑 Radio Apagada: ${socket.idNextel}`);
            delete usuariosConectados[socket.idNextel];
        }
    });
});

const PORT = 3000;
server.listen(PORT, () => console.log(`🚀 Antena en puerto ${PORT}`));