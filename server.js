const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
    cors: { origin: "*" }
});

const PORT = process.env.PORT || 3000;

// Aquí guardamos quién es quién y su estado
// Estructura: { "141*101": { socketId: "...", estado: "Disponible", hablandoCon: null } }
const usuariosConectados = {};

app.use(express.static('public'));

io.on('connection', (socket) => {
    console.log('Nueva conexión detectada:', socket.id);

    // 1. Registro del ID Fijo del teléfono
    socket.on('registrar_usuario', (idNextel) => {
        socket.idNextel = idNextel;
        usuariosConectados[idNextel] = {
            socketId: socket.id,
            estado: 'Disponible',
            hablandoCon: null
        };
        console.log(`Usuario ${idNextel} registrado y Disponible.`);
    });

    // 2. Intentar abrir canal (Pulsar PTT)
    socket.on('intentar_llamar', (idDestino) => {
        const emisor = socket.idNextel;
        const receptor = idDestino;

        // Verificar si el receptor existe en internet
        if (!usuariosConectados[receptor]) {
            socket.emit('usuario_no_disponible', { motivo: 'No conectado' });
            return;
        }

        const datosReceptor = usuariosConectados[receptor];

        // Verificar si el receptor está ocupado hablando con otro
        if (datosReceptor.estado === 'Ocupado' && datosReceptor.hablandoCon !== emisor) {
            socket.emit('error_servidor', { mensaje: 'Ocupado' });
            return;
        }

        // Si está disponible, se enlaza el canal para ambos
        usuariosConectados[emisor].estado = 'Ocupado';
        usuariosConectados[emisor].hablandoCon = receptor;
        
        usuariosConectados[receptor].estado = 'Ocupado';
        usuariosConectados[receptor].hablandoCon = emisor;

        // Avisarle al receptor quién lo está llamando
        io.to(datosReceptor.socketId).emit('canal_abierto', { por: emisor });
        socket.emit('canal_listo_para_transmitir');
    });

    // 3. Transmisión de Audio en tiempo real (Privada)
    socket.on('audio_stream', (data) => {
        const emisor = socket.idNextel;
        if (usuariosConectados[emisor] && usuariosConectados[emisor].hablandoCon) {
            const destino = usuariosConectados[emisor].hablandoCon;
            const socketDestino = usuariosConectados[destino]?.socketId;
            
            if (socketDestino) {
                // Le manda el audio únicamente al receptor vinculado
                io.to(socketDestino).emit('audio_receive', data);
            }
        }
    });

    // 4. Soltar PTT (Cerrar canal)
    socket.on('soltar_ptt', () => {
        const emisor = socket.idNextel;
        if (usuariosConectados[emisor]) {
            const destino = usuariosConectados[emisor].hablandoCon;

            // Liberar al emisor
            usuariosConectados[emisor].estado = 'Disponible';
            usuariosConectados[emisor].hablandoCon = null;

            // Liberar al receptor si seguía conectado
            if (destino && usuariosConectados[destino]) {
                usuariosConectados[destino].estado = 'Disponible';
                usuariosConectados[destino].hablandoCon = null;
                io.to(usuariosConectados[destino].socketId).emit('canal_cerrado');
            }
        }
    });

    // 5. Desconexión repentina (Se cerró la app o se quedó sin señal)
    socket.on('disconnect', () => {
        const idNextel = socket.idNextel;
        if (idNextel && usuariosConectados[idNextel]) {
            const destino = usuariosConectados[idNextel].hablandoCon;
            
            // Si estaba hablando con alguien, liberamos a la otra persona primero
            if (destino && usuariosConectados[destino]) {
                usuariosConectados[destino].estado = 'Disponible';
                usuariosConectados[destino].hablandoCon = null;
                io.to(usuariosConectados[destino].socketId).emit('canal_cerrado');
            }

            delete usuariosConectados[idNextel];
            console.log(`Usuario ${idNextel} se ha desconectado.`);
        }
    });
});

http.listen(PORT, () => {
    console.log(`Antena NetXel operando en puerto ${PORT}`);
});
