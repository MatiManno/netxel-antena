const express = require('express');
const cors = require('cors');
const { createServer } = require('http');
const { Server } = require('socket.io');

const app = express();
app.use(cors());
app.use(express.static('public'));

const httpServer = createServer(app);
// Forzamos a Socket.io a usar transporte websocket limpio para que Android no rebote
const io = new Server(httpServer, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const PORT = process.env.PORT || 3000;
const usuariosConectados = {};

io.on('connection', (socket) => {
    console.log('Celular conectado a la antena:', socket.id);

    socket.on('registrar_usuario', (idNextel) => {
        socket.idNextel = idNextel;
        usuariosConectados[idNextel] = {
            socketId: socket.id,
            estado: 'Disponible',
            hablandoCon: null
        };
        console.log(`Flota ${idNextel} dada de alta.`);
        // Le respondemos al celu que ya está registrado
        socket.emit('registro_exitoso', idNextel);
    });

    socket.on('intentar_llamar', (idDestino) => {
        const emisor = socket.idNextel;
        const receptor = idDestino;
        if (!usuariosConectados[receptor]) {
            socket.emit('usuario_no_disponible', { motivo: 'No conectado' });
            return;
        }
        const datosReceptor = usuariosConectados[receptor];
        if (datosReceptor.estado === 'Ocupado' && datosReceptor.hablandoCon !== emisor) {
            socket.emit('error_servidor', { mensaje: 'Ocupado' });
            return;
        }
        usuariosConectados[emisor].estado = 'Ocupado';
        usuariosConectados[emisor].hablandoCon = receptor;
        usuariosConectados[receptor].estado = 'Ocupado';
        usuariosConectados[receptor].hablandoCon = emisor;

        io.to(datosReceptor.socketId).emit('canal_abierto', { por: emisor });
        socket.emit('canal_listo_para_transmitir');
    });

    socket.on('audio_stream', (data) => {
        const emisor = socket.idNextel;
        if (usuariosConectados[emisor] && usuariosConectados[emisor].hablandoCon) {
            const destino = usuariosConectados[emisor].hablandoCon;
            const socketDestino = usuariosConectados[destino]?.socketId;
            if (socketDestino) {
                io.to(socketDestino).emit('audio_receive', data);
            }
        }
    });

    socket.on('soltar_ptt', () => {
        const emisor = socket.idNextel;
        if (usuariosConectados[emisor]) {
            const destino = usuariosConectados[emisor].hablandoCon;
            usuariosConectados[emisor].estado = 'Disponible';
            usuariosConectados[emisor].hablandoCon = null;
            if (destino && usuariosConectados[destino]) {
                usuariosConectados[destino].estado = 'Disponible';
                usuariosConectados[destino].hablandoCon = null;
                io.to(usuariosConectados[destino].socketId).emit('canal_cerrado');
            }
        }
    });

    socket.on('disconnect', () => {
        const idNextel = socket.idNextel;
        if (idNextel && usuariosConectados[idNextel]) {
            const destino = usuariosConectados[idNextel].hablandoCon;
            if (destino && usuariosConectados[destino]) {
                usuariosConectados[destino].estado = 'Disponible';
                usuariosConectados[destino].hablandoCon = null;
                io.to(usuariosConectados[destino].socketId).emit('canal_cerrado');
            }
            delete usuariosConectados[idNextel];
        }
    });
});

// Usamos httpServer en vez de app.listen para que Render no se maree
httpServer.listen(PORT, () => {
    console.log(`Antena Nextel corriendo en puerto ${PORT}`);
});
